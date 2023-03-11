from typing import Coroutine, Dict, Any, Callable, Awaitable, Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook


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
            self.shortname: execute
        }
    
    def copy(self):
        condition_hook = ConditionHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            order=self.order
        )

        condition_hook.stage = self.stage

        return condition_hook