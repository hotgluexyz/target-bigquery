#!/usr/bin/env python3

import argparse
import io
import json
import singer
import sys
import traceback

from target_bigquery.config import TargetConfig, TablesConfig
from target_bigquery.process import SingerProcessor
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
            logger.info(
                f"Skipping dataset validation due to insufficient permissions. Using dataset: {dataset_id}"
            )
            pass
        elif e.response.status_code == 409:  # dataset exists
            pass
        else:
            logger.critical(
                f"Unable to create dataset {dataset_id} in project {project_id}. Exception: {e}"
            )
            return 2  # sys.exit(2)

    return client, Dataset(dataset_ref)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-t", "--tables", help="Table configs file", required=False)
    parser.add_argument("-s", "--state", help="Initial state file", required=False)
    flags = parser.parse_args()

    # Process target config file
    with open(flags.config) as f:
        config_dict = json.load(f)
    config = TargetConfig(**config_dict)
    state_handler = State if config.merge_state_messages else LiteralState

    # Process target tables config file
    table_config_path = flags.tables or config.table_config
    tables_config = TablesConfig()
    if table_config_path:
        with open(table_config_path) as f:
            tables_config_dict = json.load(f)
        tables_config = TablesConfig(**tables_config_dict)

    # Load initial state
    state = None
    if flags.state is not None:
        with open(flags.state) as f:
            state = json.load(f)

    tap_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    client, dataset = ensure_dataset(
        config.project_id, config.dataset_id, config.location
    )

    try:
        processor = SingerProcessor(config, tables_config)
        for line in tap_stream:
            processor.process(line)
        processor.on_complete()

        # load_to_bigquery() -->
        # 1. For each parquet file (AKA stream), we:
        #     - compare the bq_query_schema[stream_name] with the schema of the remote bigquery target table,
        #       add missing columns using the existing `create_missing_columns` method OR create the table if it doesn't exist.
        # 2. We load all the files into google cloud storage in parallel (perhaps with a limit of some number of files at a time))
        # 3. We use `bq load` to load the parquet files into bigquery from google cloud sorage
        #     - Use the partition fields, cluster fields, and replication method from the target-tables-config.json to determine the job load config
        #     - If append or truncate, we issue a simple load job to load the parquet files into big query
        #         - Else if incremental, we load the parquet files into a temp table and then do a merge, ensuring that we clean up the temp table after the merge.

        emit_state(state)

    except Exception as e:
        # load errors surface here
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.critical(
            repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        )
        logger.critical(e)
        return 2  # sys.exit(2)

    return 0  # sys.exit(0)


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
