import json
import singer
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any, Dict, List
from datetime import datetime, UTC
from dateutil import parser as dateutil_parser

from target_bigquery.config import TargetConfig, TablesConfig, TableConfig
from target_bigquery.bigquery_schema import (
    build_schema,
    create_valid_bigquery_name,
    cleanup_record,
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


def convert_value_to_pyarrow_type(value: Any, expected_type: pa.DataType) -> Any:
    """
    Convert a value to match the expected PyArrow type.
    Handle common type conversion issues gracefully.
    Incorporates best practices from target-parquet implementation.
    """
    if value is None:
        return None

    try:
        # Handle empty strings for non-string types (key insight from target-parquet)
        if (
            isinstance(value, str)
            and value.strip() == ""
            and not pa.types.is_string(expected_type)
        ):
            return None

        # Handle timestamp conversion using more robust dateutil parser
        if pa.types.is_timestamp(expected_type):
            if isinstance(value, str):
                if value.lower() == "null":
                    return None
                try:
                    # Use dateutil parser which is much more flexible than fromisoformat
                    return dateutil_parser.parse(value)
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Cannot convert string '{value}' to timestamp: {e}, setting to None"
                    )
                    return None
            elif isinstance(value, datetime):
                return value

        # Handle integer conversion
        elif pa.types.is_integer(expected_type):
            if isinstance(value, str):
                if value.lower() == "null":
                    return None
                try:
                    # Handle potential float strings like "123.0"
                    return int(float(value))
                except (ValueError, TypeError):
                    logger.warning(
                        f"Cannot convert string '{value}' to integer, setting to None"
                    )
                    return None
            elif isinstance(value, (int, float)):
                return int(value)

        # Handle float conversion
        elif pa.types.is_floating(expected_type):
            if isinstance(value, str):
                if value.lower() == "null":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Cannot convert string '{value}' to float, setting to None"
                    )
                    return None
            elif isinstance(value, (int, float)):
                return float(value)
            elif hasattr(value, "__class__") and value.__class__.__name__ == "Decimal":
                try:
                    return float(value)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Cannot convert Decimal '{value}' to float, setting to None"
                    )
                    return None

        # Handle boolean conversion
        elif pa.types.is_boolean(expected_type):
            if isinstance(value, str):
                if value.lower() == "null":
                    return None
                value_lower = value.lower()
                if value_lower in ("true", "1", "yes", "y", "t"):
                    return True
                elif value_lower in ("false", "0", "no", "n", "f"):
                    return False
                else:
                    logger.warning(
                        f"Cannot convert string '{value}' to boolean, setting to None"
                    )
                    return None
            elif isinstance(value, bool):
                return value

        # Handle string conversion (most permissive)
        elif pa.types.is_string(expected_type):
            if value is None:
                return None
            # Handle complex objects by JSON serialization (from target-parquet)
            if isinstance(value, (list, dict)):
                try:
                    return json.dumps(value, default=str)
                except (TypeError, ValueError):
                    return str(value)
            return str(value)

        # For other types, return as-is and let PyArrow handle it
        return value

    except Exception as e:
        logger.warning(f"Error converting value '{value}' to type {expected_type}: {e}")
        return None


def convert_and_filter_record_to_pyarrow(
    record: Any, schema: pa.Schema, bq_schema_dict: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Unified function that combines schema filtering and PyArrow type conversion.
    Handles nested records and arrays recursively.

    :param record: Input record (dict, list, or primitive value)
    :param schema: PyArrow Schema defining expected structure
    :param bq_schema_dict: BigQuery schema dict for field filtering (optional)
    :return: Converted record matching PyArrow schema
    """

    # Handle non-dict records (for recursive calls with lists/primitives)
    if not isinstance(record, dict):
        if isinstance(record, list):
            # Handle array case - should have list type in schema
            # For now, return as-is and let PyArrow handle it
            return record
        else:
            # Primitive value - should not happen at top level
            return record

    converted_row = {}

    # Process each field in the PyArrow schema
    for field in schema:
        field_name = field.name
        value = record.get(field_name)

        # If field is not in record, set to None as default
        if field_name not in record:
            converted_row[field_name] = None
            continue

        if value is None:
            converted_row[field_name] = None
            continue

        # Handle different PyArrow field types
        if pa.types.is_struct(field.type):
            # Nested record - recurse
            if isinstance(value, dict):
                nested_schema = pa.schema(field.type)
                nested_bq_schema = (
                    bq_schema_dict.get(field_name, {}).get("fields", {})
                    if bq_schema_dict
                    else {}
                )
                converted_row[field_name] = convert_and_filter_record_to_pyarrow(
                    value, nested_schema, nested_bq_schema
                )
            else:
                logger.warning(
                    f"Expected dict for struct field {field_name}, got {type(value)}"
                )
                converted_row[field_name] = None

        elif pa.types.is_list(field.type):
            # Array field - convert each element
            if isinstance(value, list):
                element_type = field.type.value_type
                converted_elements = []

                for element in value:
                    if pa.types.is_struct(element_type):
                        # Array of nested records
                        nested_schema = pa.schema(element_type)
                        nested_bq_schema = (
                            bq_schema_dict.get(field_name, {}).get("fields", {})
                            if bq_schema_dict
                            else {}
                        )
                        converted_element = convert_and_filter_record_to_pyarrow(
                            element, nested_schema, nested_bq_schema
                        )
                    else:
                        # Array of primitives
                        converted_element = convert_value_to_pyarrow_type(
                            element, element_type
                        )
                    converted_elements.append(converted_element)

                converted_row[field_name] = converted_elements
            else:
                logger.warning(
                    f"Expected list for array field {field_name}, got {type(value)}"
                )
                converted_row[field_name] = None

        else:
            # Simple field - use existing conversion logic
            converted_value = convert_value_to_pyarrow_type(value, field.type)
            converted_row[field_name] = converted_value

    return converted_row


def bq_field_to_pyarrow_field(bq_field: SchemaField) -> pa.Field:
    """
    Convert a BigQuery SchemaField to a PyArrow Field

    :param bq_field: BigQuery SchemaField object
    :return: PyArrow Field
    """
    field_name = bq_field.name
    field_type = bq_field.field_type
    field_mode = bq_field.mode
    precision = bq_field.precision
    scale = bq_field.scale

    # Type mapping from BigQuery to PyArrow
    type_mapping = {
        "STRING": pa.string(),
        "INTEGER": pa.int64(),
        "FLOAT": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "DATE": pa.date32(),
        "TIME": pa.time64("us"),
        "GEOGRAPHY": pa.string(),
        "DECIMAL": pa.decimal128(precision or 38, scale or 9),
        "BIGDECIMAL": pa.decimal256(precision or 76, scale or 38),
    }

    if field_type == "RECORD":
        # Handle nested records recursively
        nested_fields = []
        for nested_field in bq_field.fields:
            nested_fields.append(bq_field_to_pyarrow_field(nested_field))

        arrow_type = pa.struct(nested_fields)
    else:
        arrow_type = type_mapping.get(field_type, pa.string())

    # Handle REPEATED mode
    if field_mode == "REPEATED":
        arrow_type = pa.list_(arrow_type)

    # Determine nullability
    nullable = field_mode == "NULLABLE"

    return pa.field(field_name, arrow_type, nullable=nullable)


def bq_schema_to_pyarrow_schema(bq_fields: List[SchemaField]) -> pa.Schema:
    """
    Convert BigQuery schema (list of SchemaFields) to PyArrow schema

    :param bq_fields: List of BigQuery SchemaField objects
    :return: PyArrow Schema
    """
    pa_fields = []
    for bq_field in bq_fields:
        pa_fields.append(bq_field_to_pyarrow_field(bq_field))

    return pa.schema(pa_fields)


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
            # Convert all rows to match PyArrow types
            try:
                converted_rows = [
                    convert_and_filter_record_to_pyarrow(
                        row,
                        self.pyarrow_schemas[stream_name],
                        self.big_query_schema_dicts[stream_name],
                    )
                    for row in self.rows[stream_name]
                ]

                # Create PyArrow table from converted rows
                table = pa.Table.from_pylist(
                    converted_rows, schema=self.pyarrow_schemas[stream_name]
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

            # Get writer and write table
            writer = self._get_parquet_writer(stream_name)
            writer.write_table(table)

            # Close writer immediately to free memory and file handles
            writer.close()
            self.parquet_writers.pop(stream_name, None)

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

        self.rows[stream_name].append(nr)
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
