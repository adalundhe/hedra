import json
import uuid

def assemble_fields_string(table_name=None, fields=None):
    return ', '.join([
            '{table}.{field}'.format(
                table=table_name,
                field=field
            ) for field in fields
        ])


def format_value(value, field_type=None):
    if type(value) == str:
        return "'{value}'".format(value=value)
    elif type(value) == dict:
        return json.dumps(value)
    elif type(value) == float or type(value) == int:
        return str(value)
    elif isinstance(value, uuid.UUID):
        return "'{value}'".format(value=str(value))
    elif type(value) == bool:
        return "{value}".format(
            value=value
        )
    elif type(value) == list:
        if len(value) < 1:
            return "ARRAY[]::{field_type}".format(field_type=field_type)

        return "ARRAY[{values_array}]".format(
            values_array=', '.join([
                "'{formatted_value}'".format(
                    formatted_value=raw_value
                ) for raw_value in value
            ])
        )
    else:
        return value


def format_values(fields=None, values=None, field_types=None):

    formatted_values = []
    for field, value in zip(fields, values):
        if value is not None:
            formatted_values.append(
                format_value(
                    value,
                    field_type=field_types.get(field)
                )
            )

    return formatted_values
