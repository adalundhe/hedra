from typing import Any, Dict, Type, Callable, Awaitable, Any, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.simple_context import SimpleContext
from .hook import Hook
from .hook_metadata import HookMetadata


class TeardownHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: Optional[str]=None,
        metadata: Dict[str, Any]={}
    ) -> None:

        if metadata is None:
            metadata = {}

        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.TEARDOWN
        )

        self.key = key
        self.metadata = HookMetadata(**metadata)

    async def call(self, context: SimpleContext):
        results = await self._call()

        if self.key:
            context[self.key] = results


        return context