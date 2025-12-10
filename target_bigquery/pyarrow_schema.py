import json
import singer
import pyarrow as pa
from typing import Any, Dict, List
from datetime import datetime
from dateutil import parser as dateutil_parser
from google.cloud.bigquery import SchemaField

logger = singer.get_logger()


def convert_value_to_pyarrow_type(value: Any, expected_type: pa.DataType) -> Any:
    """
    Convert a value to match the expected PyArrow type.
    Handle common type conversion issues gracefully.
    Incorporates best practices from target-parquet implementation.
    """
    if value is None:
        return None

    try:
        # Handle empty strings for non-string types
        if (
            isinstance(value, str)
            and value.strip() == ""
            and not pa.types.is_string(expected_type)
        ):
            return None

        # Handle timestamp conversion
        if pa.types.is_timestamp(expected_type):
            if isinstance(value, str):
                if value.lower() == "null":
                    return None
                try:
                    # Try fast ISO format parsing first (handles most cases)
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    # Fallback to slower but more flexible dateutil parser from legacy implementation
                    try:
                        return dateutil_parser.parse(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Cannot convert string '{value}' to timestamp with any parser: {e}, setting to None"
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
    record: Any,
    schema: pa.Schema,
    bq_schema_dict: Dict[str, Any] = None,
    schema_cache: Dict[str, pa.Schema] = None,
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
                # Use schema cache for nested structures
                field_type_str = str(field.type)
                if schema_cache is not None and field_type_str not in schema_cache:
                    schema_cache[field_type_str] = pa.schema(field.type)
                nested_schema = (
                    schema_cache[field_type_str]
                    if schema_cache
                    else pa.schema(field.type)
                )

                nested_bq_schema = (
                    bq_schema_dict.get(field_name, {}).get("fields", {})
                    if bq_schema_dict
                    else {}
                )
                converted_row[field_name] = convert_and_filter_record_to_pyarrow(
                    value, nested_schema, nested_bq_schema, schema_cache
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
                        # Array of nested records - use schema cache
                        element_type_str = str(element_type)
                        if (
                            schema_cache is not None
                            and element_type_str not in schema_cache
                        ):
                            schema_cache[element_type_str] = pa.schema(element_type)
                        nested_schema = (
                            schema_cache[element_type_str]
                            if schema_cache
                            else pa.schema(element_type)
                        )

                        nested_bq_schema = (
                            bq_schema_dict.get(field_name, {}).get("fields", {})
                            if bq_schema_dict
                            else {}
                        )
                        converted_element = convert_and_filter_record_to_pyarrow(
                            element, nested_schema, nested_bq_schema, schema_cache
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
