from typing import Dict, Any


class PlaceholderCall:

    def __init__(self, call_node: Dict[str, Any]) -> None:
        self.node = call_node
        self.call = call_node.get('call')
        self.is_async = call_node.get('awaitable')