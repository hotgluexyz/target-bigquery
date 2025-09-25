import json
import singer

from google.cloud.bigquery import SchemaField
from jsonschema.validators import validator_for
from target_bigquery.config import TargetConfig, TablesConfig, TableConfig
from target_bigquery.schema import build_schema, create_valid_bigquery_name
from target_bigquery.simplify_json_schema import simplify
from target_bigquery.validate_json_schema import (
    validate_json_schema_completeness,
    check_schema_for_dupes_in_field_names,
)
from typing import Any, Dict, List

logger = singer.get_logger()


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
        self.key_properties[stream_name] = message.key_properties

        # Validation of schema
        # I think we can add a check to see if we should validate here?
        validator_cls = validator_for(message.schema)
        validator_cls.check_schema(message.schema)
        self.validators[stream_name] = validator_cls(message.schema)
        validate_json_schema_completeness(self.json_schemas[stream_name])
        check_schema_for_dupes_in_field_names(
            stream_name=stream_name, schema=self.json_schemas[stream_name]
        )

        # BigQuery schema generation
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

        logger.info(
            f"Processed SCHEMA message for stream: {message.stream}. BigQuery schema: {schema}"
        )

    def handle_record_message(self, message: singer.RecordMessage):
        stream_name = message.stream
        # if stream not in self.schemas:
        #    raise Exception(f"A record for stream {msg.stream} was encountered before a corresponding schema")
        # schema = stream[stream_name].schema
        # if we are configured to validate
        #     stream[stream_name].validator.validate(msg.record, schema)
        # nr = cleanup_record(schema, msg.record)
        # nr = format_record_to_schema(nr, self.bq_schema_dicts[stream_name])
        # if add_metadata_columns:
        #     add metadata
        #
        # # This is new stuff for this implementation
        # pa_row = convert_to_pyarrow_row(nr)
        # rows[stream_name] = pa_row
        # if len(rows[stream_name]) > STREAM_ROW_CACHE_SIZE:
        #     flush to parquet file
        pass

    def handle_state_message(self, message: singer.StateMessage):
        pass

    def process(self, line: str):
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            self.handle_record_message(msg)

        elif isinstance(msg, singer.SchemaMessage):
            self.handle_schema_message(msg)

        elif isinstance(msg, singer.StateMessage):
            self.handle_state_message(msg)

        else:
            raise Exception("Unrecognized message {}".format(msg))
