from typing import Coroutine, List, Optional
from hedra.core.pipelines.hooks.types.types import HookType



class Metadata:

    def __init__(
        self, 
        weight: int = 1, 
        order: int = 1, 
        env: str = None, 
        user: str = None, 
        path: str = None,
        tags: List[str] = []
    ) -> None:
        self.weight = weight
        self.order = order
        self.env = env
        self.user = user
        self.path = path
        self.tags = tags

class Hook:

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Coroutine, 
        stage: str = None,
        hook_type=HookType.ACTION,
        names: List[str] = [], 
        metadata: Metadata = Metadata(), 
        checks: List[Coroutine]=[],
        group: Optional[str]=None
    ) -> None:
        self.name = name
        self.shortname = shortname
        self.call = call
        self.stage = stage
        self.hook_type = hook_type
        self.names = list(set(names))
        self.config = metadata
        self.checks = checks
        self.session = None
        self.action = None
        self.group = group
