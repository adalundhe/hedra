
from typing import Any, Dict, List
from hedra.parsing.actions.types.mercury_http_action import MercuryHTTPAction
from collections import defaultdict
import weakref

class Action:
    pass


class HTTPAction(Action):

    def __init__(self, name: str = None, url: str = None, method: str = None, headers: Dict[str, str] = {}, params: Dict[str, str] = None, data: Any = None, tags: List[Dict[str, str]] = []) -> None:
        self.action = MercuryHTTPAction({
            'name': name,
            'url': url,
            'method': method,
            'headers': headers,
            'params': params,
            'data': data,
            'tags': tags
        })