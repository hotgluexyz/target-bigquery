import re
import singer
from target_bigquery.schema import create_valid_bigquery_name

LOGGER = singer.get_logger()


def validate_json_schema_completeness(schema_input):
    """
    Background
        - We already do have schema validation implemented with json schema library.
        - However, we had situations when a schema had empty properties, but existing json schema validator didn't flag
            it as invalid/incomplete (it happened with Shopify schema).

    The purpose of this function is
        - to flag schema as invalid if it is incomplete.

    :param schema_input: JSON schema. It can be dictionary/JSON object or string.

    What the function does

        Clean input:
            - If it's not a string, we'll convert it to string.

        Validation steps:
            - If schema has empty {} inside its "properties", "type" or "items", fail schema completeness validation check.
                - To help the end user, we'll tell them which part of schema has empty {} (properties, type or items).
            - If schema has empty {} elsewhere, a warning is logged.

        Rationale for our validation steps:
            - empty props, type or items clearly indicate invalid/incomplete schema
            - {} elsewhere also indicates invalid/incomplete schema, it may cause pipeline failure,
                but we want to relax the rules a bit
                and we don't want to be too heavy-handed
    """

    # convert to string
    schema_input_as_string = str(schema_input)

    # strip spaces from schema file (which is treated as text)
    schema_input_no_spaces = re.sub(' |\n', '', schema_input_as_string)

    # regex match for patterns of incomplete schema
    # the purpose of the dictionary below is to create a more user-friendly error message and point the end user to the
    # correponding part of their schema (props, type, items or elsewhere)
    completeness_validation_dict_exception = {"properties": re.compile(r'\"properties\"\:\{\}'),
                                              "type": re.compile(r'\"type\"\:\[\]'),
                                              "items": re.compile(r'\"items\"\:\{\}')
                                              }

    completeness_validation_dict_warning = {
        "object/dictionary": re.compile(r'\{\}')
    }

    # raise exception
    for schema_element, pattern_not_valid in completeness_validation_dict_exception.items():

        if pattern_not_valid.search(schema_input_no_spaces):
            raise ValueError(f"JSON schema is invalid/incomplete. It has empty {schema_element}")

    # give warning
    for schema_element, pattern_not_valid in completeness_validation_dict_warning.items():

        if pattern_not_valid.search(schema_input_no_spaces):
            LOGGER.warning(f"the pipeline might fail because of undefined fields: an empty {schema_element} indicated as {{}}")


def check_schema_for_dupes_in_field_names(stream_name, schema):
    """
    Alerts user if there are duplicate field names in JSON schema.

    For example, if JSON schema contains:
        "Name" and "name" (this will be considered a dupe field in BigQuery and it'll throw an error)
        or
        "first name" and "first_name" (this example is also a dupe because "first name" will be converted to "first_name" by schema.py)
    :param stream_name: name of stream
    :param schema: JSON schema of the stream
    :return:
    """
    def build_field_list(schema):
        """

        :param schema:
        :return: a dictionary, where:

            every key is uppercase of BigQuery transformed key (uppercase of field name cleaned up to load into BigQuery)

            every  value is original field names from JSON schema

            This dictionary is flat, not nested.

            JSON nested fields are represented in dictionary with a . dot.

            This dict makes it easy to detect dupes and tell the user exactly where the dupe is located
                (what its parent field is).

        Sample output:

            f_dict / fields = {'OBJECT': ['object'], 'ID': ['id'],
                                'PERSON._SOURCE': ['person.$source'],
                                'PERSON.NAME': ['person.name', 'person.Name']}

            dupes = {'PERSON.NAME': ['person.name', 'person.Name']}

        """
        f_dict = {}
        for field_name, field_property in schema.get("properties", schema.get("items", {}).get("properties", {})).items():
            if not ("items" in field_property and "properties" in field_property["items"]) \
                    and not ("properties" in field_property):
                key = create_valid_bigquery_name(field_name.upper())
                if not f_dict.get(key):
                    f_dict[key] = [field_name]
                else:
                    f_dict[key].append(field_name)

            elif ("items" in field_property and "properties" in field_property["items"]) \
                    or ("properties" in field_property):
                nd = build_field_list(field_property)
                key = create_valid_bigquery_name(field_name.upper())
                for k, v in nd.items():
                    if not f_dict.get(f"{key}.{k}"):
                        f_dict[f"{key}.{k}"] = [f"{field_name}.{i}" for i in v]
                    else:
                        f_dict[f"{key}.{k}"].extend([f"{field_name}.{i}" for i in v])

        return f_dict

    fields = build_field_list(schema)
    dupes = {k: v for k, v in fields.items() if len(v) > 1}
    if dupes:
        errs = "; ".join([f"{' & '.join(v)} are read as {str(k)} by BigQuery" for k, v in dupes.items()])
        raise ValueError(f"Duplicate field(s) in stream {stream_name}: {errs}")














