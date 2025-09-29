#!/usr/bin/env python3

import argparse
import io
import json
import os
import singer
import sys
import traceback

from target_bigquery.config import TargetConfig, TablesConfig
from target_bigquery.process import SingerProcessor
from target_bigquery.state import State, LiteralState
from target_bigquery.biquery_loader import BigQueryLoader

from google.api_core import exceptions
from google.cloud import bigquery

logger = singer.get_logger()


def load_json_file(file_path: str, file_description: str = "file") -> dict:
    """
    Load and parse a JSON file with comprehensive error handling.

    Args:
        file_path: Path to the JSON file to load
        file_description: Description of the file type for error messages

    Returns:
        Parsed JSON data as dictionary

    Raises:
        SystemExit: On any file loading or parsing error
    """
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical(f"{file_description.capitalize()} file not found: {file_path}")
        sys.exit(2)
    except json.JSONDecodeError as e:
        logger.critical(f"Invalid JSON in {file_description} file {file_path}: {e}")
        sys.exit(2)
    except Exception as e:
        logger.critical(f"Error reading {file_description} file {file_path}: {e}")
        sys.exit(2)


def emit_state(state):
    """
    Emit Singer state to stdout and optionally to a state file.

    Writes the state as JSON to stdout (for Singer protocol compliance)
    and also to a file if TARGET_BIGQUERY_STATE_FILE environment variable is set.

    Args:
        state: State object containing bookmarks and other state information
    """
    if state is not None:
        line = json.dumps(state)
        logger.debug(f"Emitting state: {line}")
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

        if os.environ.get("TARGET_BIGQUERY_STATE_FILE", None):
            fn = os.environ.get("TARGET_BIGQUERY_STATE_FILE", None)
            with open(fn, "a") as f:
                f.write("{}\n".format(line))


def ensure_dataset(project_id, dataset_id, location):
    """
    Ensure BigQuery dataset exists, creating it if necessary.

    Attempts to create the dataset and handles common error cases:
    - 403 Forbidden: Log warning and continue (insufficient permissions)
    - 409 Conflict: Continue silently (dataset already exists)
    - Other errors: Log critical error and exit

    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID to create/verify
        location: Geographic location for the dataset (e.g., 'US')

    Returns:
        Tuple of (BigQuery client, Dataset reference)
    """
    client = bigquery.Client(project=project_id, location=location)

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.create_dataset(dataset_ref)
        logger.info(f"Successfully created BigQuery dataset: {dataset_id}")
    except exceptions.GoogleAPICallError as e:
        if e.response.status_code == 403:
            logger.info(
                f"Skipping dataset validation due to insufficient permissions - using dataset: {dataset_id}"
            )
        elif e.response.status_code == 409:  # dataset exists
            logger.info(f"BigQuery dataset {dataset_id} already exists - continuing")
        else:
            logger.critical(
                f"Failed to create BigQuery dataset {dataset_id} in project {project_id}: {e}"
            )
            raise

    return client, bigquery.Dataset(dataset_ref)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-t", "--tables", help="Table configs file", required=False)
    parser.add_argument("-s", "--state", help="Initial state file", required=False)
    flags = parser.parse_args()

    # Process target config file
    config_dict = load_json_file(flags.config, "config")
    config = TargetConfig(**config_dict)
    state_handler = State if config.merge_state_messages else LiteralState

    # Process target tables config file
    table_config_path = flags.tables or config.table_config
    tables_config = TablesConfig()
    if table_config_path:
        tables_config_dict = load_json_file(table_config_path, "tables config")
        tables_config = TablesConfig(**tables_config_dict)

    # Load initial state
    state = None
    if flags.state is not None:
        state = load_json_file(flags.state, "state")

    tap_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    ensure_dataset(config.project_id, config.dataset_id, config.location)

    try:
        with SingerProcessor(config, tables_config) as processor:
            parquet_files, key_properties = processor.process(tap_stream)

        BigQueryLoader(
            config,
            tables_config,
            parquet_files,
            key_properties,
        ).load()

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
