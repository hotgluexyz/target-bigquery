import json
import os
import sys
import unittest
import re

from google.cloud import bigquery


class BaseUnitTest(unittest.TestCase):

    def setUp(self):
        os.environ["TARGET_BIGQUERY_STATE_FILE"] = "state.json.tmp"
        self.delete_temp_state()

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.normpath(
            os.path.join(os.path.dirname(__file__), "..", "sandbox", "sa.json"))

        os.environ["TARGET_CONFIG"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target-config.json')

        os.environ["TARGET_CONFIG_CACHE"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target_config_cache.json')

        os.environ["TARGET_CONFIG_CACHE_APPEND"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target_config_cache_append.json')

        os.environ["TARGET_CONFIG_CONTAINS_TARGET_TABLES_CONFIG"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target_config_contains_target_tables_config.json')

        os.environ["MALFORMED_TARGET_CONFIG"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'malformed_target_config.json')

        os.environ["TARGET_CONFIG_MERGE_STATE_FALSE_FLAG"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target_config_merge_state_false_flag.json')

        os.environ["TARGET_CONFIG_INCREMENTAL"] = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
            'target_config_incremental.json')

        # TODO: make naming convention of target config files consistent "_" vs "-". Use "_" as it's easier to copy with a click
        # I think we would just need to rename target-config.json to target_config.json (also update it in README)
        self.client = None
        self.project_id = None
        self.dataset_id = None

    def tearDown(self):
        self.delete_temp_state()
        self.delete_dataset()

    def set_cli_args(self, ds_delete=True, *args, **kwargs):
        arg = [arg for arg in args]

        for k, v in kwargs.items():
            if k == "stdin":
                sys.stdin = open(v, "r")
                continue

            # if some flag is being passed, such as --merge-state or --no-merge-state:
            # we want to add this flag to CLI arguments
            if k == "flag":
                arg.append(v)
                continue

            arg.append("--{}".format(k))
            arg.append("{}".format(v))

        sys.argv[1:] = arg

        if "config" in kwargs and ds_delete:
            c = None
            with open(kwargs["config"], "r") as f:
                c = json.load(f)

            self.project_id = c["project_id"]
            self.dataset_id = c["dataset_id"]
            self.client = bigquery.Client(project=self.project_id)

            self.delete_dataset()

    def delete_temp_state(self):
        try:
            os.remove(os.environ["TARGET_BIGQUERY_STATE_FILE"])
        except:
            pass

    def delete_dataset(self):
        try:
            self.client.delete_dataset(
                dataset=self.dataset_id,
                delete_contents=True
            )
        except:
            pass

    def get_state(self):
        state = []
        with open(os.environ["TARGET_BIGQUERY_STATE_FILE"], "rb") as f:
            for line in f:
                state.append(json.loads(line))

        return state
