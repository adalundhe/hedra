import json
from typing import Dict, Any, Union


def normalize_headers(action_data: Dict[str, Any]) -> Dict[str, str]:

    normalized_headers = {}
    action_item_headers = action_data.get('headers', {})

    if isinstance(action_item_headers, (str, bytes, bytearray, )):
        action_item_headers = json.loads(action_item_headers)

    for header_name, header in action_item_headers.items():
        normalized_headers[header_name] = header

    return normalized_headers


def parse_data(
    action_data: Dict[str, Any],
    content_type: str
) -> Union[str, Dict[str, Any]]:

    action_item_data = action_data.get('data')
    if isinstance(action_item_data, (str, bytes, bytearray, )) and content_type == 'application/json':
        action_item_data = json.loads(action_item_data)

    return action_item_data


def parse_tags(
    action_data: Dict[str, Any]
):
    action_item_tags = action_data.get('tags', [])

    if isinstance(action_item_tags, (bytes, bytearray, )):
        action_item_tags = action_item_tags.decode()

    if isinstance(action_item_tags, str):
        action_item_tags = action_item_tags.split(',')

    return action_item_tags