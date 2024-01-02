from typing import Tuple, Dict, Any
from typing import Dict, Any


class PlaceholderCall:

    def __init__(self, call_node: Dict[str, Any]) -> None:
        self.node = call_node
        self.call = call_node.get('call')
        self.is_async = call_node.get('awaitable')

    async def to_value(
        self,
        *args,
        **kwargs
    ):
        if self.is_async:
            return await self.call(*args, **kwargs)
        
        return self.call(*args, **kwargs)