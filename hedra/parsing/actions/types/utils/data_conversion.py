import json


async def convert_data(data, method=None, mime_type=None):
    if data is None:
        return data

    converters = {
        'POST': {
            'application/json': json.dumps
        },
        'PUT': {
            'application/json': json.dumps
        }
    }

    method_converters = converters.get(method)
    if method_converters is None:
        return data

    type_converter = method_converters.get(mime_type)
    if type_converter is None:
        return data

    return type_converter(data)


async def stringify_params(params=None):
    parsed_params = {}
    if params:
        for param, value in params.items():
            if type(value) == bool:
                value = str(value)
            parsed_params[param] = value

    return parsed_params


