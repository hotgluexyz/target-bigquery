#!/usr/bin/env python3
import os
import argparse
import io
import json
import sys
import traceback
import json
import os
import sys
import singer

from target_bigquery.process import process
from target_bigquery.state import State, LiteralState
from target_bigquery.utils import emit_state

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.bigquery import Dataset


logger = singer.get_logger()


STREAM_ROW_CACHE_SIZE = 10000


def ensure_dataset(project_id, dataset_id, location):
    """
    Given a project id, dataset id and location, creates BigQuery dataset

    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html

    :param project_id, str: GCP project id from target config file. Passed to bigquery.Client().
    :param dataset_id, str: BigQuery dataset id from target config file.
    :param location, str: location for the dataset (US). Passed to bigquery.Client().
    :return: client (BigQuery Client Object) and Dataset (BigQuery dataset)
    """
    from google.cloud.bigquery import DatasetReference
    client = bigquery.Client(project=project_id, location=location)

    dataset_ref = DatasetReference(project_id, dataset_id)
    try:
        client.create_dataset(dataset_ref)
    except exceptions.GoogleAPICallError as e:
        if e.response.status_code == 403:
            logger.info(f"Skipping dataset validation due user does not have permission to create datasets. Using dataset id from config {dataset_id}")
            pass
        elif e.response.status_code == 409:  # dataset exists
            pass
        else:
            logger.critical(f"unable to create dataset {dataset_id} in project {project_id}; Exception {e}")
            return 2  # sys.exit(2)

    return client, Dataset(dataset_ref)


def main():
    parser = argparse.ArgumentParser()  # argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-t", "--tables", help="Table configs file", required=False)
    parser.add_argument("-s", "--state", help="Initial state file", required=False)
    flags = parser.parse_args()

    # read target-config file into a dict
    with open(flags.config) as f:
        config = json.load(f)

    # target tables config (e.g, partitioning and clustering)
    table_config = flags.tables or config.get("table_config")
    tables = {}
    if table_config:
        with open(table_config) as f:
            tables = json.load(f)

    # state
    state = None
    if flags.state is not None:
        with open(flags.state) as f:
            state = json.load(f)

    # Process arguments supplied in target config files
    # -------------------------------------------------
    # We process the supplied --config and --tables arguments
    #       --> We determine whether to validate and whether to add metadata columns
    #       --> We determine each streams force fields
    #       --> We determine each streams partition fields
    #       --> We determine each streams cluster fields
    #       --> We determine each streams replication method
    #       --> We determine each streams "key_properties", aka their required fields
    #       --> We read the table prefix and table suffix from the target-config.json
    #

    # Get BigQuery Info
    location = config.get("location", "US")
    project_id, dataset_id = config["project_id"], config["dataset_id"]

    # Determine replication method
    replication_method = None
    if config.get("replication_method") == "append":

        if config.get("truncate_on_full_sync") and os.environ.get("SYNC_TYPE") == "full_sync":
            replication_method = "truncate"
        else:
            replication_method = "append"
    elif config.get("replication_method") == "truncate":
        replication_method = "truncate"
    elif config.get("replication_method") == "incremental":
        replication_method = "incremental"
    else:
        replication_method = "append"

    # Get other settings
    table_prefix = config.get("table_prefix", "")
    table_suffix = config.get("table_suffix", "")
    validate_records = config.get("validate_records", True)
    add_metadata_columns = config.get("add_metadata_columns", True)
    force_alphanumeric_table_names = config.get("force_alphanumeric_table_names", False)
    state_handler = State if config.get("merge_state_messages", True) else LiteralState

    tap_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    client, dataset = ensure_dataset(project_id, dataset_id, location)


    table_config = {}

    # This stuff will be set when we encounter a schema message for a given stream_name for the first time.
    streams = {
        "stream_name": {
            "validator": "https://python-jsonschema.readthedocs.io/en/latest/api/jsonschema/protocols/#jsonschema.protocols.Validator",
            "table_name": "",
            "key_properties": "",
            "json_schema": "is just msg.schema, we use when we call validator.validate",
            "big_query_schema": "a list of BigQuery SchemaFields, which represents one BigQuery table, used for creating_missing_columns on stream end",
            "big_query_schema_dict": "created by `_build_bq_schema_dict`, used by Schema.format_record_to_schema on record messages"
        }
    }

    rows = {
        "stream_name": ["array of pyarrow rows, flushed at `STREAM_ROW_CACHE_SIZE`"]
    }



    try:
        # 2. We iterate over the data.singer file
        #
        #   for each message in data.singer:
        #     if message is a schema message:
        #         if we have not seen this schema before:
        #             assign streams.table_name (if force_alphanumeric_table_names we convert it,
        #             assign streams.validator if we are configured to validate
        #             assign streams.key_properties
        #             assign streams.json_schema
        #             assign streams.big_query_schema ~= `build_schema(json_schema_simplified, add_metadata,force_fields)`
        #             assign streams.big_query_schema_dict
        #             loggger.info(f"{msg.stream} BigQuery schema {schema}")
        #
        #       else if message is a record message:
        #           stream_name = msg.stream
        #           if stream not in self.schemas:
        #              raise Exception(f"A record for stream {msg.stream} was encountered before a corresponding schema")
        #           schema = stream[stream_name].schema
        #           if we are configured to validate
        #               stream[stream_name].validator.validate(msg.record, schema)
        #           nr = cleanup_record(schema, msg.record)
        #           nr = format_record_to_schema(nr, self.bq_schema_dicts[stream_name])
        #           if add_metadata_columns:
        #               add metadata
        #
        #           # This is new stuff for this implementation
        #           pa_row = convert_to_pyarrow_row(nr)
        #           rows[stream_name] = pa_row
        #           if len(rows[stream_name]) > STREAM_ROW_CACHE_SIZE:
        #               flush to parquet file
        #
        #       else if message is a state message:
        #         we process the state message
        #           --> For state messages, this should be pretty easy to process all state messages in the pass the way we are now
        #
        # 3. We use `bq load` to load the parquet files into big query
        #       1. For each parquet file (AKA stream), we:
        #           - compare the bq_query_schema[stream_name] with the schema of the remote bigquery target table,
        #             add missing columns using the existing `create_missing_columns` method OR create the table if it doesn't exist.
        #
        #       2. We load all the files into google cloud storage in parallel (perhaps with a limit of some number of files at a time))

        #       3. We use `bq load` to load the parquet files into bigquery from google cloud sorage
        #           - Use the partition fields, cluster fields, and replication method from the target-tables-config.json to determine the job load config
        #           - If append or truncate, we issue a simple load job to load the parquet files into big query
        #           - Else if incremental, we load the parquet files into a temp table and then do a merge, ensuring that we clean up the temp table after the merge.

        emit_state(state)

    except Exception as e:
        # load errors surface here
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.critical(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        logger.critical(e)
        return 2  # sys.exit(2)

    return 0  # sys.exit(0)


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
