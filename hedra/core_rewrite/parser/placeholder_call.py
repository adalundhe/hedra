from typing import Dict, Any


class PlaceholderCall:

    def __init__(self, call_node: Dict[str, Any]) -> None:
        self.node = call_node