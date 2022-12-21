from typing import Any, Callable, Awaitable
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ContextHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CONTEXT
        )
        
        self.context_key = key

    async def call(self, context: SimpleContext) -> None:
        context[self.context_key] = await self._call()