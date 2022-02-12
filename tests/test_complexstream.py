"""Setup:
    - Add the following files into sandbox directory under project root directory:
            - sa.json with GCP credential
            - target-config.json:
                {
                    "project_id": "{your-project-id}",
                    "dataset_id": "{your_dataset_id}"
                }
"""
from tests import unittestcore
from google.cloud.bigquery import SchemaField, Client
import json
import os
from decimal import Decimal
import pandas as pd

import logging
from testfixtures import LogCapture


class TestComplexStreamLoadJob(unittestcore.BaseUnitTest):

    def test_klaviyo_stream(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'klaviyo_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")

    def test_klaviyo_stream_load_job_should_fail_due_to_dupes_in_fiels_names(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'klaviyo_stream_contains_dupe_fields.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()

        self.assertEqual(ret, 2, msg="Exit code is not 2!")  # expected exit code is 2 - serious problem

    def test_recharge_stream(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'recharge_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")

    def test_bing_ads_stream(self):
        """
        data vs schema match here
        """
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'bing_ads_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")

    def test_bing_ads_stream_data_vs_schema_dont_match(self):
        """
        This test succeeds

        It tests if we overcame the following error in Tap Bing Ads

        JSON schema library validator flags a mismatch in data type between data and schema.

        CRITICAL 123456 is not of type 'null', 'string'

        Failed validating 'type' in schema['properties']['BillToCustomerId']:
            {'type': ['null', 'string']}

        On instance['BillToCustomerId']:
            123456

        """

        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'bing_ads_stream_schema_vs_data_have_diff_data_types.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")

    def test_complex_stream(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'facebook_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")
        # self.assertDictEqual(state, {"bookmarks": {"simple_stream": {"timestamp": "2020-01-11T00:00:00.000000Z"}}})
        #
        # table = self.client.get_table("{}.simple_stream_dev".format(self.dataset_id))
        # self.assertEqual(3, table.num_rows, msg="Number of rows mismatch")
        # self.assertIsNone(table.clustering_fields)
        # self.assertIsNone(table.partitioning_type)

    def test_complex_stream_decimal_schema_valid(self):

        # DATA AND CONFIG
        input_file = os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'data'), 'facebook_stream_decimal_test_schema_valid.json')

        config_file = os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                   'target-config.json')

        # VERIFY INPUTS
        data = []
        with open(input_file) as f:
            for line in f:
                data.append(json.loads(line))

        # verify input data and schema
        assert data[0]['schema']['properties']["budget_remaining"]["multipleOf"] == 1e-03
        assert data[1]['record']["budget_remaining"] == 5000000.1
        assert data[2]['record']["budget_remaining"] == 5000000.12
        assert data[3]['record']["budget_remaining"] == 57573500.123
        assert data[4]['record']["budget_remaining"] == 2450980.0
        assert data[5]['record']["budget_remaining"] == 2450980.0

        # RUN SYNC
        from target_bigquery import main

        self.set_cli_args(
            stdin=input_file,
            config=config_file,
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)
        self.assertEqual(ret, 0, msg="Exit code is not 0!")

        # VERIFY OUTPUTS

        config = json.load(open(config_file))
        project_id = config["project_id"]
        dataset_id = config["dataset_id"]

        bq_client = Client(project=project_id)
        bq_schemas_dict = {}

        # Make an API request.
        tables_http_iterator = bq_client.list_tables(project_id + '.' + dataset_id)

        for table_list_item in tables_http_iterator:
            table = bq_client.get_table(
                f"{table_list_item.project}.{table_list_item.dataset_id}.{table_list_item.table_id}")

            bq_schemas_dict.update({table.table_id: table.schema})

        # verify schema
        stream = "adsets"
        try:
            test_field = bq_schemas_dict[stream][7]
        except:
            stream = stream + "_dev"
            test_field = bq_schemas_dict[stream][7]

        assert test_field.name == "budget_remaining"
        assert test_field.field_type in ["NUMERIC", "DECIMAL"]  # NUMERIC is the same as DECIMAL
        assert test_field.precision == 32
        assert test_field.scale == 3

        # verify data

        query_string = f"SELECT budget_remaining FROM `{project_id}.{dataset_id}.{stream}`"

        dataframe = (
            bq_client.query(query_string)
                .result()
                .to_dataframe()
        )
        actual = dataframe["budget_remaining"]
        expected = pd.Series([Decimal('2450980'), Decimal('2450980'), Decimal('5000000.1'),
                              Decimal('5000000.12'), Decimal('57573500.123')])

        assert actual.equals(expected)

    def test_complex_stream_decimal_schema_invalid(self):
        file = os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'data'), 'facebook_stream_decimal_test_schema_invalid.json')

        data = []
        with open(file) as f:
            for line in f:
                data.append(json.loads(line))

        assert data[0]['schema']['properties']["budget_remaining"]["multipleOf"] == 1e-02
        assert data[1]['record']["budget_remaining"] == 5000000.1
        assert data[2]['record']["budget_remaining"] == 5000000.12
        assert data[3]['record']["budget_remaining"] == 57573500.123

        from target_bigquery import main

        self.set_cli_args(
            stdin=file,
            config=os.path.join(
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                'target-config.json'),
            processhandler="load-job"
        )
        ret = main()
        self.assertEqual(ret, 2, msg="Exit code is not 2!")  # load must fail
        # weird error: Failed validating 'type' in schema['properties']['daily_budget']:
        # I changed nothing about daily_budget, I made budget_remaining invalid

    def test_complex_stream_with_tables_config(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'facebook_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            tables=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'config'), 'facebook_stream_tables_config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")
        # self.assertDictEqual(state, {"bookmarks": {"simple_stream": {"timestamp": "2020-01-11T00:00:00.000000Z"}}})
        #
        # table = self.client.get_table("{}.simple_stream_dev".format(self.dataset_id))
        # self.assertEqual(3, table.num_rows, msg="Number of rows mismatch")
        # self.assertIsNotNone(table.clustering_fields)
        # self.assertIsNotNone(table.partitioning_type)

    def test_complex_stream_with_tables_config_force_field(self):
        """
        the purpose of this test is to make sure that if you supply date_start field
            in Facebook ads_insights_age_and_gender streams
            as a required string,
            build_schema function will force this field to NULLABLE DATE, according to target tables config file
        """
        target_config_file = json.load(open(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'config'), 'facebook_stream_tables_config.json')))

        assert target_config_file['streams']['ads_insights_age_and_gender']['force_fields']['date_start'][
                   'type'] == 'DATE'
        assert target_config_file['streams']['ads_insights_age_and_gender']['force_fields']['date_start'][
                   'mode'] == 'NULLABLE'

        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'facebook_stream_date_start_is_required_string.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            tables=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'config'), 'facebook_stream_tables_config.json'),
            processhandler="load-job"
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")
        # self.assertDictEqual(state, {"bookmarks": {"simple_stream": {"timestamp": "2020-01-11T00:00:00.000000Z"}}})

        table = self.client.get_table("{}.ads_insights_age_and_gender".format(self.dataset_id))
        self.assertEqual(15, table.num_rows, msg="Number of rows mismatch")
        self.assertIsNotNone(table.clustering_fields)
        self.assertIsNotNone(table.partitioning_type)

        actual = table.schema[42]

        expected = SchemaField(name='date_start',
                               field_type='DATE',
                               mode='NULLABLE',
                               description=None,
                               fields=(),
                               policy_tags=None)

        assert actual == expected

    def test_misformed_complex_stream(self):
        """
        Note that the target config's "validate_records" flag should be set to False

        sandbox/malformed_target_config.json:

            {
                "project_id": "{your-project-id}",
                "dataset_id": "{your_dataset_id}",
                "validate_records":  false
            }

        """

        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
                'data'), 'facebook_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'malformed_target_config.json'),
            processhandler="load-job",
        )

        ret = main()
        state = self.get_state()[-1]
        print(state)

        self.assertEqual(ret, 0, msg="Exit code is not 0!")


    def test_schema_error(self):

        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(os.path.join(
                os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'),
                             'rsc'),
                'data'), 'stream_format_record_to_schema_fails.json'),
            config=os.path.join(
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                'target-config.json'),
            processhandler="load-job",
        )

        ret = main()

        self.assertEqual(ret, 2, msg="Exit code is not 2!")

    def test_schema_logging(self):

        """
        Test logging as part of this pull request QA: https://github.com/adswerve/target-bigquery/pull/27
        # Feature/improve schema logging #27

        About testing logging:
        https://testfixtures.readthedocs.io/en/latest/logging.html
        https://stackoverflow.com/questions/13733552/logger-configuration-to-log-to-file-and-print-to-stdout
        """

        from target_bigquery import main, logger

        with LogCapture() as actual_logs:
            # make sure logs are displayed during local testing in console
            # make sure unit test logs are in the same format as what we see during the sync
            log_formatter = logging.Formatter("%(levelname)s %(message)s")
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_formatter)
            logger.addHandler(console_handler)

            # set level
            console_handler.setLevel(logging.INFO)

            # test log
            logger.info("unit test starts")

            # run sync
            self.set_cli_args(
                stdin=os.path.join(os.path.join(
                    os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'),
                                 'rsc'),
                    'data'), 'stream_format_record_to_schema_fails.json'),
                config=os.path.join(
                    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                    'target-config.json'),
                processhandler="load-job",
            )

            ret = main()

            # verify that sync did not succeed
            self.assertEqual(ret, 2, msg="Exit code is not 2!")
            # test log
            logger.info("unit test ends")

            # verify logs
            actual_logs.check_present(
                ('root', 'INFO', "unit test starts"),
                ('root', 'INFO',
                 "simple_stream schema: {'properties': {'id': {'type': ['string']}, 'name': {'type': ['null', 'string']}, 'orderindex': {'type': ['null', 'integer']}, 'override_statuses': {'type': ['null', 'boolean']}, 'hidden': {'type': ['null', 'boolean']}, 'space': {'properties': {'id': {'type': ['null', 'string']}, 'name': {'type': ['null', 'string']}}, 'type': 'object'}, 'task_count': {'type': ['null', 'string', 'integer']}, 'statuses': {'items': {}, 'type': ['array', 'null']}, 'lists': {'items': {}, 'type': ['array', 'null']}, 'archived': {'type': ['null', 'boolean']}, 'permission_level': {'type': ['null', 'string']}}, 'type': 'object'}"),
                ('root', 'CRITICAL',
                 "Cannot format a record for stream simple_stream to its corresponding BigQuery schema. Details: {'record': {'id': '12933951', 'name': 'Milestone and Project Plan', 'orderindex': 17, 'override_statuses': 'false', 'hidden': 'false', 'space': {'id': '2577684', 'name': 'meshDelivery'}, 'task_count': '10', 'archived': 'true', 'statuses': [], 'lists': [{'id': '25670974', 'name': 'POC <customer/department>', 'orderindex': 0, 'status': 'null', 'priority': 'null', 'assignee': 'null', 'task_count': 10, 'due_date': 'null', 'start_date': 'null', 'space': {'id': '2577684', 'name': 'meshDelivery', 'access': 'true'}, 'archived': 'false', 'override_statuses': 'null', 'statuses': [{'id': 'p2577684_eDZ87cTk', 'status': 'Open', 'orderindex': 0, 'color': '#d3d3d3', 'type': 'open'}, {'id': 'p2577684_Sf8kB74x', 'status': 'planned', 'orderindex': 1, 'color': '#82CB11', 'type': 'custom'}, {'id': 'p2577684_yG5b2doG', 'status': 'in progress', 'orderindex': 2, 'color': '#4194f6', 'type': 'custom'}, {'id': 'p2577684_BZKpph7f', 'status': 'review', 'orderindex': 3, 'color': '#A875FF', 'type': 'custom'}, {'id': 'p2577684_ouoISXPV', 'status': 'Closed', 'orderindex': 4, 'color': '#6bc950', 'type': 'closed'}], 'permission_level': 'create'}], 'permission_level': 'create'}, 'schema': {'properties': {'id': {'type': ['string']}, 'name': {'type': ['null', 'string']}, 'orderindex': {'type': ['null', 'integer']}, 'override_statuses': {'type': ['null', 'boolean']}, 'hidden': {'type': ['null', 'boolean']}, 'space': {'properties': {'id': {'type': ['null', 'string']}, 'name': {'type': ['null', 'string']}}, 'type': 'object'}, 'task_count': {'type': ['null', 'string', 'integer']}, 'statuses': {'items': {}, 'type': ['array', 'null']}, 'lists': {'items': {}, 'type': ['array', 'null']}, 'archived': {'type': ['null', 'boolean']}, 'permission_level': {'type': ['null', 'string']}}, 'type': 'object'}, 'bq_schema': {'id': {'type': 'STRING', 'mode': 'REQUIRED', 'policyTags': {'names': []}}, 'name': {'type': 'STRING', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'orderindex': {'type': 'INTEGER', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'override_statuses': {'type': 'BOOLEAN', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'hidden': {'type': 'BOOLEAN', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'space': {'type': 'RECORD', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'fields': {'id': {'type': 'STRING', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'name': {'type': 'STRING', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}}}, 'task_count': {'type': 'STRING', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'statuses': {'type': 'RECORD', 'mode': 'REPEATED', 'precision': None, 'scale': None, 'fields': []}, 'lists': {'type': 'RECORD', 'mode': 'REPEATED', 'precision': None, 'scale': None, 'fields': []}, 'archived': {'type': 'BOOLEAN', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, 'permission_level': {'type': 'STRING', 'mode': 'NULLABLE', 'precision': None, 'scale': None, 'policyTags': {'names': []}}, '_time_extracted': {'type': 'timestamp', 'mode': 'NULLABLE', 'policyTags': {'names': []}}, '_time_loaded': {'type': 'timestamp', 'mode': 'NULLABLE', 'policyTags': {'names': []}}}}"),
                ('root', 'INFO', "unit test ends")
            )

