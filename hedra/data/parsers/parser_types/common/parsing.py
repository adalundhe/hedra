import json
from typing import Dict, Any, Union


def normalize_headers(action_data: Dict[str, Any]) -> Dict[str, str]:

    normalized_headers = {}
    action_item_headers = action_data.get('headers', {})
    for header_name, header in action_item_headers.items():
        normalized_headers[header_name] = header

    return normalized_headers


def parse_data(
    action_data: Dict[str, Any],
    content_type: str
) -> Union[str, Dict[str, Any]]:

    action_item_data = action_data.get('data')
    if content_type.lower() == 'application/json' and action_item_data:
        action_item_data = json.loads(action_item_data)

    return action_item_data