import json
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
from jsonschema.validators import validator_for

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
        # Make a copy to avoid mutating the original field
        import copy
        f = copy.deepcopy(f)
        schema_dict[f["name"]] = f
        if f.get("fields"):
            schema_dict[f["name"]]["fields"] = build_bq_schema_dict(f["fields"])
        schema_dict[f["name"]].pop("description")
        schema_dict[f["name"]].pop("name")
    return schema_dict


class SingerProcessor:
    def __init__(self, target_config: TargetConfig, tables_config: TablesConfig):
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

    def _get_parquet_writer(self, stream_name: str) -> pq.ParquetWriter:
        """Get or create a ParquetWriter for the given stream"""
        if stream_name not in self.parquet_writers:
            # Create parquet file path
            parquet_file = f"{stream_name}.parquet"
            self.parquet_files[stream_name] = parquet_file

            # Create ParquetWriter with predefined schema
            self.parquet_writers[stream_name] = pq.ParquetWriter(
                parquet_file, self.pyarrow_schemas[stream_name], compression="snappy"
            )

        return self.parquet_writers[stream_name]

    def _write_batch_to_parquet(self, stream_name: str):
        """Write accumulated rows to parquet file and clear the buffer"""
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

            logger.info(
                f"Successfully wrote batch of {row_count} rows to {stream_name}.parquet"
            )

        except Exception as e:
            logger.error(
                f"Error writing batch to parquet for stream {stream_name}: {e}"
            )
            raise

    def _flush_all_streams(self):
        """Flush all streams that have data to their parquet files"""
        streams_flushed = []
        for stream_name in self.rows:
            if self.rows[stream_name]:  # Only flush streams with data
                self._write_batch_to_parquet(stream_name)
                streams_flushed.append(stream_name)

        # Reset the total counter after flushing all streams
        self.total_cached_rows = 0

        if streams_flushed:
            logger.info(
                f"Flushed {len(streams_flushed)} streams: {', '.join(streams_flushed)}"
            )

    def _close_parquet_writers(self):
        """Close all parquet writers and finalize files"""
        for stream_name, writer in self.parquet_writers.items():
            try:
                writer.close()
                logger.info(f"Closed parquet writer for stream {stream_name}")
            except Exception as e:
                logger.error(
                    f"Error closing parquet writer for stream {stream_name}: {e}"
                )

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
            validator_cls = validator_for(message.schema)
            validator_cls.check_schema(message.schema)
            self.validators[stream_name] = validator_cls(message.schema)

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
        validator = self.validators[stream_name]

        if self.target_config.validate_records:
            validator.validate(message.record, schema)

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
        )

        self.rows[stream_name].append(converted_row)
        self.total_cached_rows += 1

        # Flush all streams when total cached rows exceed threshold
        if self.total_cached_rows >= STREAM_ROW_CACHE_SIZE:
            self._flush_all_streams()

    def handle_state_message(self, message: singer.StateMessage):
        pass

    def process(self, line: str):
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(message, singer.RecordMessage):
            self.handle_record_message(message)

        elif isinstance(message, singer.SchemaMessage):
            self.handle_schema_message(message)

        elif isinstance(message, singer.StateMessage):
            self.handle_state_message(message)

        else:
            raise Exception("Unrecognized message {}".format(message))

    def on_complete(self):
        """Flush any remaining rows to parquet files and close writers"""
        # Flush remaining rows for all streams
        if self.total_cached_rows > 0:
            self._flush_all_streams()

        # Close all parquet writers
        self._close_parquet_writers()

        logger.info("Completed processing and wrote all parquet files")
