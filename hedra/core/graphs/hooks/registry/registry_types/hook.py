import uuid
from hedra.logging import HedraLogger
from typing import Any, Callable, Awaitable
from hedra.core.graphs.hooks.hook_types.hook_type import HookType


class Hook:

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Callable[..., Awaitable[Any]], 
        stage: str = None,
        hook_type=HookType.ACTION
    ) -> None:
        self.hook_id = str(uuid.uuid4())
        self.name = name
        self.shortname = shortname
        self._call: Callable[..., Awaitable[Any]] = call
        self.stage = stage
        self.hook_type = hook_type
        self.stage_instance: Any = None
