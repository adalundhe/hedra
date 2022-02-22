import json
from .data_conversion import convert_data

  

async def parse_action(action, headers):

    action_headers = action.get('headers')
    if action_headers is None:
        action_headers = {}
    headers.update(action_headers)


    data = await convert_data(
        action.get('data'),
        method=action.get('method'),
        mime_type=action.get('Content-Type')
    )

    return {
        'name': action.get('name'),
        'host': action.get('host'),
        'user': action.get('user'),
        'tags': action.get('tags'),
        'url': '{host}{endpoint}'.format(
            host=action.get('host'),
            endpoint=action.get('endpoint')
        ),
        'auth': action.get('auth'),
        'params': action.get('params'),
        'endpoint': action.get('endpoint'),
        'method': action.get('method'),
        'headers': headers,
        'data': data
    }
