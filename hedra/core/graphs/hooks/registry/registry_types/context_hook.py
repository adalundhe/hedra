from typing import Any, Callable, Awaitable, Optional
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ContextHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        store_key: str,
        load_key: Optional[str]=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CONTEXT
        )
        
        self.store_key = store_key
        self.load_key = load_key

    async def call(self, context: SimpleContext) -> None:

        if self.load_key:
            context[self.store_key] = await self.call(
                context[self.load_key]
            )

        else:
            context[self.store_key] = await self._call()

        return context