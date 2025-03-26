import singer
import json
import copy
import os
from decimal import Decimal

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from target_bigquery.schema import build_schema, prioritize_one_data_type_from_multiple_ones_in_any_of, \
    convert_field_type, determine_precision_and_scale_for_decimal_or_bigdecimal, create_valid_bigquery_name

from tests.schema_old import build_schema_old

from target_bigquery.simplify_json_schema import simplify
from tests import unittestcore

from tests.utils import convert_list_of_schema_fields_to_list_of_lists, compare_old_vs_new_schema_conversion, \
    flatten_list

from target_bigquery.validate_json_schema import validate_json_schema_completeness

from tests.rsc.schemas.input_json_schemas import *

from tests.rsc.schemas.input_json_schemas_shopify import *

list_of_schema_inputs = [test_schema_collection_anyOf_problem_column,
                         schema_nested_1,
                         schema_nested_1_subset_items_problem,
                         schema_nested_2,
                         schema_nested_3_shopify,
                         shopify_orders_fixed,
                         # shopify_customers, #old schema.py fails on this in my test. New one works
                         shopify_custom_collections,
                         # shopify_abandoned_checkouts_fixed, #old schema.py fails on this in my test. New one works
                         shopify_products,
                         shopify_transactions,
                         # shopify_metafields_malformed, # not valid schema
                         shopify_metafields_fixed,
                         shopify_order_refunds,
                         shopify_collects
                         ]


class TestHelpersFunctions(unittestcore.BaseUnitTest):

    def test_utils_flatten_list(self):
        list_nested = [[1, 2], [1, 2, 3, 4, [3, 1, 3]]]

        flat = flatten_list(list_nested)

        assert flat == [1, 2, 1, 2, 3, 4, 3, 1, 3]

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_string(self):
        test_input = {
            'anyOf': [
                {
                    'type': [
                        'integer',
                        'null'
                    ]
                },
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                },
                {
                    'type': [
                        'string',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'number',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "string"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "STRING"

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_float(self):
        test_input = {
            'anyOf': [
                {
                    'type': [
                        'number',
                        'null'
                    ]
                },
                {
                    'type': [
                        'integer',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "number"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "FLOAT"

    def test_prioritize_one_data_type_from_multiple_ones_in_any_of_integer(self):
        test_input = {
            'anyOf': [

                {
                    'type': [
                        'integer',
                        'null'
                    ]
                }
                ,
                {
                    'type': [
                        'boolean',
                        'null'
                    ]
                }
            ]
        }

        prioritized_data_type = prioritize_one_data_type_from_multiple_ones_in_any_of(test_input)

        assert prioritized_data_type == "integer"

        converted_data_type = convert_field_type(test_input)

        assert converted_data_type == "INTEGER"

    # TODO: in schema.py, we're calling determine_precision_and_scale_for_decimal_or_bigdecimal twice. Optimize it, so we only call it once

    def test_scale_decimal_1(self):
        input = {"daily_budget": {
            "type": [
                "null",
                "number"
            ],
            "multipleOf": 0.01
        }}
        field_property = list(input.values())[0]
        precision, scale = determine_precision_and_scale_for_decimal_or_bigdecimal(field_property)

        assert scale == 2
        assert precision == 31

    def test_scale_decimal_2(self):
        input = {"daily_budget": {
            "type": [
                "null",
                "number"
            ],
            "multipleOf": 0.00001
        }}

        field_property = list(input.values())[0]
        precision, scale = determine_precision_and_scale_for_decimal_or_bigdecimal(field_property)

        assert scale == 5
        assert precision == 34

    def test_scale_decimal_3(self):
        input = {"daily_budget": {
            "type": [
                "null",
                "number"
            ],
            "multipleOf": 1e-06
        }}

        field_property = list(input.values())[0]
        precision, scale = determine_precision_and_scale_for_decimal_or_bigdecimal(field_property)

        assert scale == 6
        assert precision == 35

    def test_scale_decimal_4(self):
        input = {"daily_budget": {
            "type": [
                "null",
                "number"
            ],
            "multipleOf": 1e-040  # lowercase e
        }}

        field_property = list(input.values())[0]
        precision, scale = determine_precision_and_scale_for_decimal_or_bigdecimal(field_property)

        assert scale == 38
        assert precision == 76

    def test_scale_decimal_5(self):
        input = {"daily_budget": {
            "type": [
                "null",
                "number"
            ],
            "multipleOf": 1E-010  # uppercase E
        }}

        field_property = list(input.values())[0]
        precision, scale = determine_precision_and_scale_for_decimal_or_bigdecimal(field_property)

        assert scale == 10
        assert precision == 48

    def test_create_valid_bigquery_name(self):
        very_long_name = "table_name_test"*40

        input_outputs = [
            # check that it replaces special characters with _
            ("t%ab*le_te&st", "t_ab_le_te_st"),
            ("t ab@le_te~st", "t_ab_le_te_st"),
            # must start with a letter or underscore
            ("0table_name", "_0table_name"),
            # check starting with reserved keywords
            ("_TABLE_", "__TABLE_"),
            ("_TABLE_test", "__TABLE_test"),
            ("_TABLE_tes%t", "__TABLE_tes_t"),
            ("_FILE_test", "__FILE_test"),
            ("_PARTITION_test", "__PARTITION_test"),
            ("_ROW_TIMESTAMP_test", "__ROW_TIMESTAMP_test"),
            ("__ROOT__test", "___ROOT__test"),
            # check truncates to max size
            (very_long_name, very_long_name[:300])
        ]
        for input_output in input_outputs:
            assert create_valid_bigquery_name(input_output[0]) == input_output[1]


class TestSchemaConversion(unittestcore.BaseUnitTest):

    def setUp(self):
        super(TestSchemaConversion, self).setUp()

    def test_flat_schema(self):

        schema_0_input = schema_simple_1

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        for f in schema_3_built_old_method:

            if f.name == "geo":
                self.assertEqual(f.field_type.upper(), "STRING")

            elif f.name == "amount":
                self.assertEqual(f.field_type.upper(), "FLOAT")

            elif f.name == "big_amount":
                self.assertEqual(f.field_type.upper(), "FLOAT")

        for f in schema_2_built_new_method:
            if f.name == "id":
                self.assertEqual(f.field_type.upper(), "STRING")

            elif f.name == "name":
                self.assertEqual(f.field_type.upper(), "STRING")

            elif f.name == "value":
                self.assertEqual(f.field_type.upper(), "INTEGER")

            elif f.name == "ratio":
                self.assertEqual(f.field_type.upper(), "FLOAT")

            elif f.name == "timestamp":
                self.assertEqual(f.field_type.upper(), "TIMESTAMP")

            elif f.name == "date":
                self.assertEqual(f.field_type.upper(), "DATE")

            elif f.name == "geo":
                self.assertEqual(f.field_type.upper(), "GEOGRAPHY")

            elif f.name == "amount_0":
                # we can only conclude that we need to output DECIMAL or BIGDECIMAL, if "multipleOf" is a float or decimal
                assert msg.schema["properties"][f.name] == {'type': ['number', 'null'], 'multipleOf': 1}

                self.assertEqual(f.field_type.upper(), "INTEGER")
                self.assertEqual(f.precision, None)
                self.assertEqual(f.scale, None)

            elif f.name == "amount_1":
                assert msg.schema["properties"][f.name] == {'type': ['number', 'null'], 'multipleOf': Decimal('0.01')}

                self.assertEqual(f.field_type.upper(), "DECIMAL")
                self.assertEqual(f.precision, 31)
                self.assertEqual(f.scale, 2)

            elif f.name == "amount_2":
                assert msg.schema["properties"][f.name] == {'type': ['number', 'null'], 'multipleOf': Decimal('1E-9')}

                self.assertEqual(f.field_type.upper(), "DECIMAL")
                self.assertEqual(f.precision, 38)
                self.assertEqual(f.scale, 9)

            elif f.name == "big_amount_1":
                assert msg.schema["properties"][f.name] == {'type': ['number', 'null'], 'multipleOf': Decimal('1E-10')}

                self.assertEqual(f.field_type.upper(), "BIGDECIMAL")
                self.assertEqual(f.precision, 48)
                self.assertEqual(f.scale, 10)

            elif f.name == "big_amount_2":
                assert msg.schema["properties"][f.name] == {'type': ['number', 'null'], 'multipleOf': Decimal('1E-50')}

                self.assertEqual(f.field_type.upper(), "BIGDECIMAL")
                self.assertEqual(f.precision, 76)
                self.assertEqual(f.scale, 38)

            elif f.name == "bq_deci":
                assert msg.schema["properties"][f.name] == {'type': 'string', 'format': 'bq-decimal'}

                self.assertEqual(f.field_type.upper(), "DECIMAL")
                self.assertEqual(f.precision, None)
                self.assertEqual(f.scale, None)

            elif f.name == "bq_bigdeci":
                assert msg.schema["properties"][f.name] == {'type': 'string', 'format': 'bq-bigdecimal'}

                self.assertEqual(f.field_type.upper(), "BIGDECIMAL")
                self.assertEqual(f.precision, None)
                self.assertEqual(f.scale, None)

    def test_one_nested_schema_1(self):

        schema_0_input = schema_nested_1

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        # are results of the two methods above identical?
        assert schema_2_built_new_method == schema_3_built_old_method

        for f in schema_2_built_new_method:
            if f.name in ("date_start", "date_stop"):
                self.assertEqual(f.field_type.upper(), "TIMESTAMP")

    def test_one_nested_schema_2(self):

        schema_0_input = schema_nested_2

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

        # are results of the two methods above identical? ignore order of columns and case
        schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_2_built_new_method)

        schema_built_old_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_3_built_old_method)

        assert schema_built_new_method_sorted == schema_built_old_method_sorted

    def test_several_nested_schemas(self):

        for next_schema_input in list_of_schema_inputs:
            schema_0_input = next_schema_input

            msg = singer.parse_message(schema_0_input)

            schema_1_simplified = simplify(msg.schema)

            schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                     add_metadata=True)

            schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties,
                                                         add_metadata=True)

            # are results of the two methods above identical? ignore order of columns and case
            schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_2_built_new_method)

            schema_built_old_method_sorted = convert_list_of_schema_fields_to_list_of_lists(schema_3_built_old_method)

            flat_new = flatten_list(schema_built_new_method_sorted)
            flat_old = flatten_list(schema_built_old_method_sorted)

            # compare the flat lists
            # each item should be equal, except for the case when the new schema has DECIMAL
            for i in range(0, len(flat_new)):
                if flat_new[i] in ("DECIMAL", "BIGDECIMAL"):
                    assert flat_old[i] == "FLOAT"
                else:
                    assert flat_new[i] == flat_old[i]

    def test_decimal_nested_field(self):
        """confirm that DECIMAL data type gets inferred correctly from "multipleOf" when you field is nested"""
        # convert schema
        schema_0_input = test_schema_collection_anyOf_problem_column

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        # check input
        assert msg.schema["properties"]["line_items"]["anyOf"][0]["items"]["properties"]["price"] == {
            'type': ['null', 'number'], 'multipleOf': Decimal('1E-8')}

        # check output
        output_field = schema_2_built_new_method[4].fields[2]

        assert output_field.name == "price"
        assert output_field.field_type == "DECIMAL"
        assert output_field.precision == 37
        assert output_field.scale == 8

    def test_several_nested_schemas_amazon(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_amazon.json'))

    def test_several_nested_schemas_asana(self):

        """
        for some reason recharge product stream schema conversion fails on the old schema.py
        works with the new schema conversion
        """

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_asana.json'), exclude_stream='workspaces')

    def test_several_nested_schemas_asana_workspaces_new_method(self):

        catalog = json.load(open(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_asana.json')))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == 'workspaces':  # old conversion fails here

                validate_json_schema_completeness(next_schema_input)

                schema_0_input = copy.deepcopy(next_schema_input)

                schema_0_input.update({"type": "SCHEMA"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # old conversion fails on asana workspaces
                # schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

                # are results of the two methods above identical? ignore order of columns and case
                schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(
                    schema_2_built_new_method)

                assert schema_built_new_method_sorted

            # TODO: check data types in this test and subsequent ones

    def test_several_nested_schemas_bing_ads(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_bing_ads.json'))

    def test_several_nested_schemas_facebook(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_facebook.json"), ignore_float_vs_decimal_bigdecimal_difference=True)

    def test_facebook_decimal(self):

        catalog = json.load(open((os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_facebook.json"))))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == "adsets":

                validate_json_schema_completeness(next_schema_input)

                # clean up schema formatting
                schema_0_input = copy.deepcopy(next_schema_input)

                if "type" not in schema_0_input.keys():
                    schema_0_input.update({"type": "SCHEMA"})

                if "key_properties" not in schema_0_input.keys():
                    schema_0_input.update({"key_properties": "Id"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                # convert schema using new method
                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # check input
                assert next_schema_input["schema"]["properties"]["daily_budget"] == {'type': ['null', 'number'],
                                                                                     'maximum': 100000000000000000000000000000000,
                                                                                     'minimum': -100000000000000000000000000000000,
                                                                                     'multipleOf': 1e-06,
                                                                                     'exclusiveMaximum': False,
                                                                                     'exclusiveMinimum': False}

                # check intermediate step
                assert schema_1_simplified["properties"]["daily_budget"] == {'type': ['string', 'null'],
                                                                             'format': 'number',
                                                                             'multipleOf': Decimal('0.000001')}
                # check output
                test_field = schema_2_built_new_method[6]
                assert test_field.name == "daily_budget"
                assert test_field.field_type == "DECIMAL"
                assert test_field.precision == 35
                assert test_field.scale == 6

    def test_several_nested_schemas_google_search_console(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_google_search_console.json"),
            ignore_float_vs_decimal_bigdecimal_difference=True)

    def test_google_search_console_bigdecimal(self):

        catalog = json.load(open((os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_google_search_console.json"))))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == "performance_report_page":

                validate_json_schema_completeness(next_schema_input)

                # clean up schema formatting
                schema_0_input = copy.deepcopy(next_schema_input)

                if "type" not in schema_0_input.keys():
                    schema_0_input.update({"type": "SCHEMA"})

                if "key_properties" not in schema_0_input.keys():
                    schema_0_input.update({"key_properties": "Id"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                # convert schema using new method
                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # check input
                assert next_schema_input["schema"]["properties"]["ctr"] == {'multipleOf': 1e-25,
                                                                            'type': ['null', 'number']}

                # check intermediate step
                assert schema_1_simplified["properties"]["ctr"] == {'type': ['string', 'null'], 'format': 'number',
                                                                    'multipleOf': Decimal('1E-25')}

                # check outputs
                test_field = schema_2_built_new_method[6]
                assert test_field.name == "ctr"
                assert test_field.field_type == "BIGDECIMAL"
                assert test_field.precision == 63
                assert test_field.scale == 25

    def test_several_nested_schemas_hubspot(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_hubspot.json"))

    def test_several_nested_schemas_klaviyo(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_klaviyo.json"))

    def test_several_nested_schemas_klaviyo_field_names_contain_chars_not_valid_in_bigquery(self):

        catalog_schema_file = os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tests'), 'rsc'),
            'schemas'), 'input_json_schemas_klaviyo_field_names_contain_chars_invalid_in_bigquery.json')

        catalog = json.load(open(catalog_schema_file))

        for next_schema_input in catalog['streams']:

            if next_schema_input['stream'] != "only_test_this_stream_skip_others":
                continue

            # make sure stream doesn't have empty (undefined object {}) type or properties
            validate_json_schema_completeness(next_schema_input)

            # clean up schema formatting
            schema_0_input = copy.deepcopy(next_schema_input)

            if "type" not in schema_0_input.keys():
                schema_0_input.update({"type": "SCHEMA"})

            if "key_properties" not in schema_0_input.keys():
                schema_0_input.update({"key_properties": "Id"})

            schema_0_input = str(schema_0_input)

            schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                "false").replace("None",
                                                                                                                 "null")

            # convert schema using old vs. new method
            msg = singer.parse_message(schema_0_input)

            schema_1_simplified = simplify(msg.schema)

            schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                     add_metadata=True)

            schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties,
                                                         add_metadata=True)

            assert schema_3_built_old_method[0].name == "$dollarsign"
            assert schema_3_built_old_method[1].name == "contains space"
            assert schema_3_built_old_method[2].name == "contains:colon"
            assert schema_3_built_old_method[3].name == "contains&ampersand"

            assert schema_2_built_new_method[0].name == "_dollarsign"
            assert schema_2_built_new_method[1].name == "contains_space"
            assert schema_2_built_new_method[2].name == "contains_colon"
            assert schema_2_built_new_method[3].name == "contains_ampersand"

    def test_several_nested_schemas_mailchimp(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_mailchimp_fixed.json"))

    def test_several_nested_schemas_recharge(self):

        """
        for some reason recharge product stream schema conversion fails on the old schema.py
        works with the new schema conversion
        """

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_recharge.json"), exclude_stream='products',
            ignore_float_vs_decimal_bigdecimal_difference=True)

    def test_several_nested_schemas_recharge_products_new_method(self):

        catalog = json.load(open(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_recharge.json")))

        for next_schema_input in catalog['streams']:

            if next_schema_input['tap_stream_id'] == 'products':  # old conversion fails here

                validate_json_schema_completeness(next_schema_input)

                schema_0_input = copy.deepcopy(next_schema_input)

                schema_0_input.update({"type": "SCHEMA"})

                schema_0_input = str(schema_0_input)

                schema_0_input = schema_0_input.replace("\'", "\"").replace("True", "true").replace("False",
                                                                                                    "false").replace(
                    "None", "null")

                msg = singer.parse_message(schema_0_input)

                schema_1_simplified = simplify(msg.schema)

                schema_2_built_new_method = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                         add_metadata=True)

                # old conversion fails on asana workspaces
                # schema_3_built_old_method = build_schema_old(msg.schema, key_properties=msg.key_properties, add_metadata=True)

                # are results of the two methods above identical? ignore order of columns and case
                schema_built_new_method_sorted = convert_list_of_schema_fields_to_list_of_lists(
                    schema_2_built_new_method)

                assert schema_built_new_method_sorted

    def test_several_nested_schemas_salesforce(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_salesforce.json"))

    def test_several_nested_schemas_shopify(self):

        compare_old_vs_new_schema_conversion(os.path.join(os.path.join(
            os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests"), "rsc"),
            "schemas"), "input_json_schemas_shopify.json"), ignore_float_vs_decimal_bigdecimal_difference=True)

    def test_invalid_characters_in_schema(self):

        schema_0_input = schema_invalid_characters_1

        msg = singer.parse_message(schema_0_input)

        schema_1_simplified = simplify(msg.schema)

        schema_2_built = build_schema(schema_1_simplified, key_properties=msg.key_properties,
                                                 add_metadata=True)

        assert schema_2_built[0].name == "i_d"
        assert schema_2_built[1].name == "na_me"
