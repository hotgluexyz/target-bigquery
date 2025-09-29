import copy
import json
import os
import io
import singer
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any, Dict, List
from datetime import datetime

from target_bigquery.config import TargetConfig, TablesConfig, TableConfig
from target_bigquery.bigquery_schema import (
    build_schema,
    create_valid_bigquery_name,
)
from target_bigquery.pyarrow_schema import (
    convert_and_filter_record_to_pyarrow,
    bq_schema_to_pyarrow_schema,
)
from target_bigquery.simplify_json_schema import simplify
from target_bigquery.validate_json_schema import (
    validate_json_schema_completeness,
    check_schema_for_dupes_in_field_names,
)

from google.cloud.bigquery import SchemaField
import fastjsonschema

logger = singer.get_logger()

STREAM_ROW_CACHE_SIZE = 75000


def cleanup_record(schema, record):
    """
    Recursively sanitize field names in a record to comply with BigQuery naming conventions.

    This function processes incoming Singer tap records by transforming all field names
    to meet BigQuery column naming requirements. It handles nested objects and arrays
    recursively, ensuring all field names are valid BigQuery identifiers.

    BigQuery naming rules applied:
    - Replace non-alphanumeric characters (except underscores) with underscores
    - Ensure field names start with letter or underscore
    - Truncate to 300 characters maximum
    - Avoid reserved prefixes

    :param schema: JSON schema for the record (currently unused but kept for API compatibility)
    :param record: Raw record data from Singer tap (dict, list, or primitive value)
    :return: Record with sanitized field names that comply with BigQuery naming conventions
    :raises Exception: If record contains unsupported data types
    """
    if not isinstance(record, dict) and not isinstance(record, list):
        return record

    elif isinstance(record, list):
        nr = []
        for item in record:
            nr.append(cleanup_record(schema, item))
        return nr

    elif isinstance(record, dict):
        nr = {}
        for key, value in record.items():
            nkey = create_valid_bigquery_name(key)
            nr[nkey] = cleanup_record(schema, value)
        return nr

    else:
        raise Exception(f"unhandled instance of record: {record}")


def build_table_name(
    stream_name: str, prefix: str, suffix: str, force_alphanumeric_table_names: bool
):
    table_name = "{}{}{}".format(prefix, stream_name, suffix)
    if force_alphanumeric_table_names:
        return create_valid_bigquery_name(table_name)
    else:
        return table_name


def build_bq_schema_dict(schema):
    """
    Convert BigQuery schema as a list to BigQuery schema as a dictionary

    :param schema, list of BigQuery SchemaFields
    :return: schema_dict, dict. Dict of BigQuery schema fields.
        Dict key is field name
        Dict value is a dict also. It has BigQuery mode and type
    """
    schema_dict = {}
    for field in schema:
        f = field if isinstance(field, dict) else field.to_api_repr()

        f = copy.deepcopy(f)
        schema_dict[f["name"]] = f
        if f.get("fields"):
            schema_dict[f["name"]]["fields"] = build_bq_schema_dict(f["fields"])
        schema_dict[f["name"]].pop("description")
        schema_dict[f["name"]].pop("name")
    return schema_dict


class SingerProcessor:
    """
    Processes Singer tap messages and converts them to Parquet files.

    This class handles the complete pipeline from Singer JSON messages to
    parquet files ready for BigQuery loading:
    1. Schema message processing and BigQuery schema generation
    2. Record message processing with validation and conversion
    3. Batched writing to parquet files with PyArrow
    4. Stream management and resource cleanup
    """

    def __init__(self, target_config: TargetConfig, tables_config: TablesConfig):
        """
        Initialize the Singer message processor.

        Args:
            target_config: Target configuration settings
            tables_config: Table-specific configuration settings
        """
        self.target_config = target_config
        self.tables_config = tables_config

        # Stream metadata
        self.table_names: Dict[str, str] = {}
        self.key_properties: Dict[str, List[str]] = {}
        self.json_schemas: Dict[str, Dict[str, Any]] = {}
        self.validators: Dict[str, Any] = {}
        self.big_query_schemas: Dict[str, List[SchemaField]] = {}
        self.big_query_schema_dicts: Dict[str, Dict[str, Any]] = {}
        self.rows: Dict[str, List[Any]] = {}

        # Parquet writers and files
        self.parquet_writers: Dict[str, pq.ParquetWriter] = {}
        self.parquet_files: Dict[str, str] = {}
        self.pyarrow_schemas: Dict[str, pa.Schema] = {}

        # Total row counter across all streams
        self.total_cached_rows = 0

        # PyArrow schema cache for nested structures
        self.nested_schema_cache: Dict[str, pa.Schema] = {}

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures parquet writers are closed."""
        self._close_parquet_writers()
        return False  # Don't suppress exceptions

    def _get_parquet_writer(self, stream_name: str) -> pq.ParquetWriter:
        """
        Get or create a ParquetWriter for the given stream.

        Creates a new ParquetWriter with predefined schema and compression
        if one doesn't exist for the stream. Stores the absolute file path
        for later reference.

        Args:
            stream_name: Name of the data stream

        Returns:
            ParquetWriter instance for the stream
        """
        if stream_name not in self.parquet_writers:
            # Create parquet file path
            parquet_file = f"{stream_name}.parquet"
            parquet_file_path = os.path.abspath(parquet_file)
            self.parquet_files[stream_name] = parquet_file_path

            # Create ParquetWriter with predefined schema
            self.parquet_writers[stream_name] = pq.ParquetWriter(
                parquet_file, self.pyarrow_schemas[stream_name], compression="snappy"
            )

        return self.parquet_writers[stream_name]

    def _write_batch_to_parquet(self, stream_name: str):
        """Write accumulated rows to parquet file and clear the buffer.

        Converts the buffered rows to a PyArrow table and writes to the
        stream's parquet file. Includes detailed error reporting if conversion fails.

        Args:
            stream_name: Name of the data stream to flush
        """
        if not self.rows[stream_name]:
            return

        try:
            table = pa.Table.from_pylist(
                self.rows[stream_name], schema=self.pyarrow_schemas[stream_name]
            )

        except Exception as conversion_error:
            # Log detailed information about the data that's causing issues
            logger.error(
                f"Error converting or creating PyArrow table for stream {stream_name}: {conversion_error}"
            )

            # Try to identify which column is causing the issue
            schema_fields = self.pyarrow_schemas[stream_name]
            sample_row = self.rows[stream_name][0] if self.rows[stream_name] else {}

            for field in schema_fields:
                field_name = field.name
                field_type = field.type
                sample_value = sample_row.get(field_name)

                logger.error(
                    f"Field: {field_name}, Expected type: {field_type}, Sample value: {sample_value} (type: {type(sample_value)})"
                )

            raise conversion_error

        try:
            # Get writer and write table
            writer = self._get_parquet_writer(stream_name)
            writer.write_table(table)

            # Clear the buffer
            row_count = len(self.rows[stream_name])
            self.rows[stream_name] = []

            logger.info(f"Wrote {row_count} rows to {stream_name}.parquet")

        except Exception as e:
            logger.error(f"Failed to write parquet batch for {stream_name}: {e}")
            raise

    def _flush_all_streams(self):
        """Flush all streams that have accumulated data to their parquet files.

        Iterates through all streams and writes any buffered rows to disk.
        Resets the total row counter after flushing.
        """
        streams_flushed = []
        for stream_name in self.rows:
            if self.rows[stream_name]:  # Only flush streams with data
                self._write_batch_to_parquet(stream_name)
                streams_flushed.append(stream_name)

        self.total_cached_rows = 0

        if streams_flushed:
            logger.info(
                f"Flushed {len(streams_flushed)} streams to parquet: {', '.join(streams_flushed)}"
            )

    def _close_parquet_writers(self):
        """Close all parquet writers and finalize files.

        Ensures all parquet files are properly closed and metadata is written.
        Should be called when all data processing is complete.
        """
        for stream_name, writer in self.parquet_writers.items():
            try:
                writer.close()
            except Exception as e:
                logger.error(f"Failed to close parquet writer for {stream_name}: {e}")

        self.parquet_writers.clear()

    def handle_schema_message(self, message: singer.SchemaMessage):
        stream_name = message.stream
        if stream_name in self.table_names:
            return

        self.table_names[stream_name] = build_table_name(
            stream_name,
            self.target_config.table_prefix,
            self.target_config.table_suffix,
            self.target_config.force_alphanumeric_table_names,
        )

        self.json_schemas[stream_name] = message.schema
        validate_json_schema_completeness(self.json_schemas[stream_name])
        check_schema_for_dupes_in_field_names(
            stream_name=stream_name, schema=self.json_schemas[stream_name]
        )

        self.key_properties[stream_name] = message.key_properties

        # Get schema validator for stream
        if self.target_config.validate_records:
            try:
                self.validators[stream_name] = fastjsonschema.compile(message.schema)
            except Exception as e:
                logger.error(f"Invalid JSON schema for stream {stream_name}: {e}")
                raise

        # Generate BigQuery schema for stream
        schema_simplified = simplify(self.json_schemas[stream_name])
        schema = build_schema(
            schema=schema_simplified,
            key_properties=self.key_properties[stream_name],
            add_metadata=self.target_config.add_metadata_columns,
            force_fields=self.tables_config.streams.get(
                stream_name, TableConfig()
            ).force_fields,
        )
        self.big_query_schemas[stream_name] = schema
        self.big_query_schema_dicts[stream_name] = build_bq_schema_dict(schema)

        self.rows[stream_name] = []

        # Create PyArrow schema from BigQuery SchemaFields
        self.pyarrow_schemas[stream_name] = bq_schema_to_pyarrow_schema(schema)

        logger.info(f"Processed SCHEMA message for stream: {message.stream}")

    def handle_record_message(self, message: singer.RecordMessage):
        stream_name = message.stream

        if stream_name not in self.json_schemas:
            raise Exception(
                f"A record for stream {stream_name} was encountered before a corresponding schema"
            )

        schema = self.json_schemas[stream_name]

        if self.target_config.validate_records:
            validator = self.validators[stream_name]
            try:
                validator(message.record)
            except fastjsonschema.JsonSchemaException as e:
                logger.error(f"Record validation failed for stream {stream_name}: {e}")
                raise

        nr = cleanup_record(schema, message.record)

        if self.target_config.add_metadata_columns:
            nr["_time_extracted"] = (
                message.time_extracted.isoformat()
                if message.time_extracted
                else datetime.utcnow().isoformat()
            )
            nr["_time_loaded"] = datetime.utcnow().isoformat()

        converted_row = convert_and_filter_record_to_pyarrow(
            nr,
            self.pyarrow_schemas[stream_name],
            self.big_query_schema_dicts[stream_name],
            self.nested_schema_cache,
        )

        self.rows[stream_name].append(converted_row)
        self.total_cached_rows += 1

        if self.total_cached_rows >= STREAM_ROW_CACHE_SIZE:
            self._flush_all_streams()

    def handle_state_message(self, message: singer.StateMessage):
        pass

    def process_line(self, line: str):
        """
        Process a single line from the Singer tap stream.

        Parses the JSON message and routes it to the appropriate handler
        based on message type (SCHEMA, RECORD, or STATE).

        Args:
            line: Raw JSON line from Singer tap
        """
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error(f"Failed to parse JSON message: {line.strip()}")
            raise

        if isinstance(message, singer.RecordMessage):
            self.handle_record_message(message)

        elif isinstance(message, singer.SchemaMessage):
            self.handle_schema_message(message)

        elif isinstance(message, singer.StateMessage):
            self.handle_state_message(message)

        else:
            raise Exception("Unrecognized message {}".format(message))

    def process(self, tap_stream: io.TextIOWrapper):
        """
        Process the complete Singer tap stream.

        Reads lines from the tap stream, processes each message,
        and returns the generated parquet files when complete.

        Args:
            tap_stream: Text stream containing Singer messages

        Returns:
            Dictionary mapping stream names to parquet file paths
        """
        for line in tap_stream:
            self.process_line(line)

        return self.on_complete(), self.key_properties

    def on_complete(self):
        """
        Complete processing and finalize all parquet files.

        Flushes any remaining buffered rows to disk, closes all parquet writers,
        and returns the mapping of stream names to their parquet file paths.

        Returns:
            Dictionary mapping stream names to absolute parquet file paths
        """

        if self.total_cached_rows > 0:
            self._flush_all_streams()

        self._close_parquet_writers()

        logger.info("Completed processing and wrote all parquet files")

        return self.parquet_files
