#!/usr/bin/env python3
import os
import argparse
import io
import json
import sys
import traceback

import singer

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.process import process
from target_bigquery.utils import emit_state, ensure_dataset
from target_bigquery.state import State, LiteralState

logger = singer.get_logger()


def main():
    # parse command line arguments (e.g., target config file path, state, table config file path, process handler type)
    parser = argparse.ArgumentParser()  # argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-t", "--tables", help="Table configs file", required=False)
    parser.add_argument("-s", "--state", help="Initial state file", required=False)

    # https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
    parser.add_argument('--merge_state_messages', help="Merge many state messages to construct a state file",
                        dest='merge_state_messages', action='store_true')
    parser.add_argument('--no-merge_state_messages',
                        help="Don't merge many state messages into one message. The latest state message becomes the state file.",
                        dest='merge_state_messages', action='store_false')
    parser.set_defaults(merge_state_messages=None)
    # default needs to be None. If it's None, it means it's not supplied and we need to check the config file
    # if default is True here, then setting it in config file will not work
    # in the config file, default will be True

    parser.add_argument("-ph", "--processhandler",
                        help="Defines the loading process. Partial loads by default.",
                        required=False,
                        choices=["load-job", "partial-load-job", "bookmarks-partial-load-job"],
                        default="partial-load-job"
                        )

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

    # determine replication method: append, truncate or incremental
    truncate = False
    incremental = False
    if config.get("replication_method", "append").lower() == "truncate" or (config.get("truncate_on_full_sync") and os.environ.get("SYNC_TYPE") == "full_sync"):
        truncate = True
    elif config.get("replication_method", "append").lower() == "incremental":
        incremental = True

    # arguments supplied in target config
    table_prefix = config.get("table_prefix", "")
    table_suffix = config.get("table_suffix", "")
    location = config.get("location", "US")
    validate_records = config.get("validate_records", True)
    add_metadata_columns = config.get("add_metadata_columns", True)
    force_alphanumeric_table_names = config.get("force_alphanumeric_table_names", False)

    merge_state_messages_cli = flags.merge_state_messages

    # we can pass merge state option via config file per Meltano request
    merge_state_messages_config = config.get("merge_state_messages", True)

    # merge state option via CLI trumps one passed via config file
    # we need to check if CLI option was passed at all. if not, we check the config file
    merge_state_messages = merge_state_messages_cli if type(
        merge_state_messages_cli) == bool else merge_state_messages_config

    project_id, dataset_id = config["project_id"], config["dataset_id"]

    table_configs = tables.get("streams", {})
    max_cache = 1024 * 1024 * config.get("max_cache", 50)  # this is needed for partial loads

    tap_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    client, dataset = ensure_dataset(project_id, dataset_id, location)

    try:
        from target_bigquery.processhandler import LoadJobProcessHandler, PartialLoadJobProcessHandler, \
            BookmarksStatePartialLoadJobProcessHandler

        # determine type of process handler
        ph = None

        if flags.processhandler == "load-job":
            ph = LoadJobProcessHandler
        elif flags.processhandler == "partial-load-job":
            ph = PartialLoadJobProcessHandler
        elif flags.processhandler == "bookmarks-partial-load-job":
            ph = BookmarksStatePartialLoadJobProcessHandler
        else:
            raise Exception("Unknown process handler.")


        # Approach for parquet based processing:
        #
        #
        # 0. The executor will convert the data.singer to a series of parquet files in the executors/target-output directory
        #       --> An undesirable side effect of this is that our disk usage will increase to be data.singer size + parquet files size, as opposed to just the data.singer size.
        #           --> This could be an issue for large datasets. We could only avoid this if we somehow deleted the processed records from data.singer every N lines in a batch like way.
        #               --> Before we try and optimize this, we should check if it is a problem since parquet files are very small compared to data.singer.
        #       --> IMPORTANT: For us to avoid doing the parquet construction in this code base, we will ned to make some changes to target-parquet.
        #           --> We need to guarantee that the schema provided by the parquet files is enough to create the schema in big query, which might not be true, for the following reasons:
        #               - We do some schema simplification in our current code, and target-parquet does not (double check this),
        #               - We support more types here than in target-parquet. Our supported types
        #                   conversion_dict = {
        #                              "string": "STRING",
        #                              "number": "FLOAT",
        #                              "integer": "INTEGER",
        #                              "boolean": "BOOLEAN",
        #                              "date-time": "TIMESTAMP",
        #                              "date": "DATE",
        #                              "time": "TIME",
        #                              "object": "RECORD",
        #                              "array": "RECORD",
        #                              "bq-geography": "GEOGRAPHY",
        #                              "bq-decimal": "DECIMAL",
        #                              "bq-bigdecimal": "BIGDECIMAL"
        #                   }
        #           --> We already know about force_fields, metadata columns, primarykey, and key properties, and validation, which we have a plan to handle (see below)
        # 1. We process target-tables-config.json and target-config.json
        #       --> We determine each streams force fields
        #       --> We determine each streams partition fields
        #       --> We determine each streams cluster fields
        #       --> We determine each streams replication method
        #       --> We determine each streams "key_properties", aka their required fields
        #       --> We read the table prefix and table suffix from the target-config.json
        # 2. We iterate over the data.singer file



        # 2. We use `bq load` to load the parquet files into big query
        #       --> For each parquet file (AKA stream):
        #           --> We compare the schema in parquet to the schema in big query, generate a diff, and issue the alter table statements we need
        #           --> We load the file into google cloud storage
        #           --> We use the partition fields, cluster fields, and replication method above to determine the job load config
        #           --> We load the parquet files into big query from google cloud storage
        #           --> If append or truncate, we issue a simple load job to load the parquet files into big query
        #           --> Else if incremental, we load the parquet files into a temp table and then do a merge, ensuring that we clean up the temp table after the merge.
        #       --> IMPORTANT: we will lose the following behavior by not doing a pass over ever message here:
        #           -->  We lose merging of state messages. Maybe we don't need to support them. We certainly don't need them to issue tmp table loads anymore. For us, merge_state_messages will always be True, so if we do support it, we use the `State` class
        #               --> POSSIBLE SOLUTIONS: Add this to target-parquet, drop support, or process in the additional pass over the data.singer file
        #           -->  We lose validation of records.
        #       --> This option will drop support for `force_fields` provided by table config, since these are applied at schema message processing time when we construct a stream schema
        #               --> POSSIBLE SOLUTIONS: Add this to target-parquet, or process in the additional pass over the data.singer file
        #       --> This option will drop support for `force_alphanumeric_table_names`, since this is applied at schema message processing time
        #               --> RECOMMENDED SOLUTION: Add this to target-parquet
        #       --> This option will drop support for `add_metadata_columns`, since this is applied at schema message processing time (and also written to at record message processing time)
        #               --> RECOMMENDED SOLUTION: Add this to target-parquet
        #       --> This option will drop support for `validate_records`, since this is applied at record message processing time (maybe these are things that we also want to add to the target-parquet)
        #           --> We also lose `format_record_to_schema`
        #           --> We also lose `cleanup_record`
        #           --> We also lose `schema_simplification`
        #           --> POSSIBLE SOLUTIONS: target-parquet has validation, extend it/replace it. Add option to ignore, or process in the additional pass over the data.singer file
        #       --> For all these options, if we do not add support to target-parquet, we will need to do one pass over the data.singer file to them
        #           --> For adding metadata columns, this would mean we would need to implement batched processing to limit file writes
        #           --> For state messages, this should be pretty easy to process all state messages in the pass the way we are now
        #           --> For validation/cleaning, we would have to do it pretty much the way we are now on a per record message basis
        #           --> For alphanumeric table names, we would have to do it pretty much the way we are now on a per schema message basis

        # DECISION: We will not do above approach. Instead we will centralize the processing of the data.singer into the target-bigquery code base, copying whatever code we need from target-parquet:
        # DECISION: We will try to avoid doing multiple passes over the data.singer file, and instead do one pass that is aware of the schema messages and record messages.
        # DECISION: We will keep the validation, simplifcation, and format (see in code notes for some more details on what we drop)
        # DECISION: We will make a separate branch
        # TODO: Come to an understanding on how primary key is handled currently,
        # TODO: Rewrite algo with one pass


        # In our existing use of target-parquet in our executor, since we are not passing the cli option, processhandler will always be the default of partial-load-job.
        # max_cache is not relevant to our new implementation
        state_iterator = process(
            ph,

            tap_stream,

            initial_state=state,
            state_handler=State if merge_state_messages else LiteralState,

            project_id=project_id,
            dataset=dataset,
            location=location,

            validate_records=validate_records,
            table_prefix=table_prefix,
            table_suffix=table_suffix,
            add_metadata_columns=add_metadata_columns,
            force_alphanumeric_table_names=force_alphanumeric_table_names,

            truncate=truncate,
            incremental=incremental,

            table_configs=table_configs,


            max_cache=max_cache,
        )

        # write a state file
        for state in state_iterator:
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
