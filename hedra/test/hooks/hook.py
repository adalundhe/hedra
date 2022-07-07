from typing import Any, Coroutine, Dict, List, Union
from hedra.test.hooks.types import HookType



class Metadata:

    def __init__(
        self, 
        weight: int = 1, 
        order: int = 1, 
        env: str = None, 
        user: str = None, 
        action_type: str = None, 
        tags: List[str] = []
    ) -> None:
        self.weight = weight
        self.order = order
        self.env = env
        self.user = user
        self.type = action_type
        self.tags = tags

class Hook:

    def __init__(
        self, name: str, 
        call: Coroutine, 
        hook_type=HookType.ACTION,
        names: List[str] = [], 
        metadata: Metadata = Metadata(), 
        checks: List[Coroutine]=[]
    ) -> None:
        self.name = name
        self.call = call
        self.names = list(set(names))
        self.hook_type = hook_type
        self.config = metadata
        self.checks = checks
        self.session = None
        self.action = None
