from typing import Coroutine, Dict, Any, Callable, Awaitable, Type, Optional, Tuple
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ConditionHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CONDITION
        )

        self.names = list(set(names))
        self.pre: bool = True
        self.key: str = None
        self.order: int = order
        self.events: Dict[str, Coroutine] = {}

    async def call(self, **kwargs):
        execute = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(execute, dict):
            return {
                **kwargs,
                **execute
            }

        return {
            **kwargs,
            'execute': execute
        }
        