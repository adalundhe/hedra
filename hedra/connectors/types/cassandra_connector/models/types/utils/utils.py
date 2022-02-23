import json
import uuid

def assemble_fields_string(table_name=None, fields=None):
    return ', '.join([
            '{table}.{field}'.format(
                table=table_name,
                field=field
            ) for field in fields
        ])


def format_value(value):
    if type(value) == str:
        return "'{value}'".format(value=value)
    elif type(value) == dict:
        return json.dumps(value)
    else:
        return str(value)


def format_values(values=None):
    return [format_value(value) for value in values if value is not None]
    