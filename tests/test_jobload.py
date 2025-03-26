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
import os


class TestJobLoad(unittestcore.BaseUnitTest):

    def test_simple_stream(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                       'tests'), 'rsc'), 'data'), 'simple_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        self.assertEqual(ret, 0, msg="Exit code is not 0!")

        self.delete_dataset()
        print(self.get_state())

    def test_simple_stream_with_invalid_characters(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                       'tests'), 'rsc'), 'data'), 'simple_stream_with_invalid_characters.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        self.assertEqual(ret, 0, msg="Exit code is not 0!")

        self.delete_dataset()
        print(self.get_state())

    def test_simple_stream_with_tables_config(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                       'tests'), 'rsc'), 'data'), 'simple_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                             'sandbox'), 'target-config.json'),
            tables=os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                       'tests'), 'rsc'), 'config'), 'simple_stream_table_config.json'),
            processhandler="load-job"
        )

        ret = main()
        self.assertEqual(ret, 0, msg="Exit code is not 0!")

        self.delete_dataset()
        print(self.get_state())

    def test_complex_stream(self):
        from target_bigquery import main

        self.set_cli_args(
            stdin=os.path.join(
                os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                       'tests'), 'rsc'), 'data'), 'klaviyo_stream.json'),
            config=os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sandbox'),
                                'target-config.json'),
            processhandler="load-job"
        )

        ret = main()
        self.assertEqual(ret, 0, msg="Exit code is not 0!")

        self.delete_dataset()
        print(self.get_state())
