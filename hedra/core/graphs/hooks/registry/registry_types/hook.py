import uuid
from typing import Coroutine, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType


class Hook:

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Coroutine, 
        stage: str = None,
        hook_type=HookType.ACTION
    ) -> None:
        self.hook_id = str(uuid.uuid4())
        self.name = name
        self.shortname = shortname
        self.call = call
        self.stage = stage
        self.hook_type = hook_type
        self.stage_instance: Any = None
        
