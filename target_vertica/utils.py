import sys
import json
import ast
import re
import itertools
import inflection
import collections

from decimal import Decimal
from singer import get_logger
from datetime import datetime

LOGGER = get_logger('target_vertica')


# ========= STREAM UTILS BELOW =========
def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {
        'type': ['null', 'string']}

    return extended_schema_message


def add_metadata_values_to_record(record_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get(
        'record', {}).get('_sdc_deleted_at')

    return extended_record


def emit_state(state):
    """Emit state message to standard output then it can be
    consumed by other components"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state %s', line)
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# ========= DBSYNC UTILS BELOW  =========
def validate_config(config):
    """Validate configuration"""
    errors = []
    required_config_keys = [
        'host',
        'port',
        'user',
        'password',
        'dbname'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(
                "Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append(
            "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    return errors


def column_type(schema_property, with_length=True):
    # TODO: Data type for semi-structured data like json, xml. Vertica uses flex table for the same.
    """Take a specific schema property and return the vertica equivalent column type"""

    DEFAULT_VARCHAR_LENGTH = 80
    LONG_VARCHAR_LENGTH = 65000

    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'varchar'
    varchar_length = DEFAULT_VARCHAR_LENGTH
    if schema_property.get('maxLength', 0) > varchar_length:
        varchar_length = LONG_VARCHAR_LENGTH
    if 'object' in property_type or 'array' in property_type:
        if schema_property.get('maxLength', 0) > LONG_VARCHAR_LENGTH:
            col_type = 'long varchar'
            varchar_length = '1048576'
        else:
            varchar_length = LONG_VARCHAR_LENGTH

    # Every date-time JSON value is currently mapped to TIMESTAMP
    elif property_format == 'date-time':
        col_type = 'timestamp'
    elif property_format == 'time':
        col_type = 'time'
    elif 'number' in property_type:
        col_type = 'numeric'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'varchar'
    elif 'integer' in property_type:
        if 'maximum' in schema_property:
            if schema_property['maximum'] <= 32767:
                col_type = 'smallint'
            elif schema_property['maximum'] <= 2147483647:
                col_type = 'int'
            elif schema_property['maximum'] <= 9223372036854775807:
                col_type = 'bigint'
        else:
            col_type = 'int'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    # Add max length to column type if required
    if with_length:
        if col_type == 'varchar' and varchar_length > 0:
            col_type = '{}({})'.format(col_type, varchar_length)

    LOGGER.debug(
        "schema_property: %s -> col_type: %s", schema_property, col_type)

    return col_type


def safe_column_name(name):
    """Generate SQL friendly column name"""
    return '"{}"'.format(name).lower()


def column_clause(name, schema_property):
    """Generate DDL column name with column type string"""
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 63 and reducer_index < len(inflected_key):
        reduced_key = re.sub(
            r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) >
             1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


# pylint: disable=dangerous-default-value,invalid-name
def flatten_schema(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []

    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(
                    v, parent_key + [k], sep=sep, level=level + 1, max_level=max_level).items())
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if list(v.values())[0][0]['type'] == 'string':
                    list(v.values())[0][0]['type'] = ['null', 'string']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'array':
                    list(v.values())[0][0]['type'] = ['null', 'array']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'object':
                    list(v.values())[0][0]['type'] = ['null', 'object']
                    items.append((new_key, list(v.values())[0][0]))

    def key_func(item): 
        return item[0]

    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError(
                'Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


# pylint: disable=redefined-outer-name
def _should_json_dump_value(key, value, flatten_schema=None):
    if isinstance(value, (dict, list)):
        return True

    if flatten_schema and key in flatten_schema and 'type' in flatten_schema[key]\
            and set(flatten_schema[key]['type']) == {'null', 'object', 'array'}:
        return True

    return False


# pylint: disable-msg=too-many-arguments
def flatten_record(d, flatten_schema=None, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping) and level < max_level:
            items.extend(flatten_record(v, flatten_schema, parent_key + [k], sep=sep, level=level + 1,
                                        max_level=max_level).items())
        else:
            items.append((new_key, json.dumps(
                v) if _should_json_dump_value(k, v, flatten_schema) else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    """Generate list of SQL friendly PK column names"""
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


def stream_name_to_dict(stream_name, separator='-'):
    """Transform stream name string to dictionary"""
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = '_'.join(s[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name
    }


def format_json(data, ordered=True):
    # INFO: Maybe not required if semi structured data is stored in flex table.
    """Format string into json/dictionary/list."""
    if data and isinstance(data, list) and isinstance(data[0], dict):
        for index, item in enumerate(data.copy()):
            for k, v in dict(item).items():
                if (isinstance(v, str) and (v[0] == '[' and v[-1] == ']'
                                            or v[0] == '{' and v[-1] == '}')):
                    try:
                        data[index][k] = ast.literal_eval(v)
                    except ValueError:
                        pass
        if not ordered:
            for index, item in enumerate(data.copy()):
                data[index] = dict(item)
    return data
