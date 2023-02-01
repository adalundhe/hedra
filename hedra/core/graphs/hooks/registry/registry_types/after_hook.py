from typing import Any, List, Type, Callable, Awaitable
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class AfterHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: List[str]
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.AFTER
        )

        self.names = list(set(names))

    async def call(self, **kwargs):
        result = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(result, dict):
            return {
                **kwargs,
                **result
            }

        action, response = result 

        return {
            **kwargs,
            'action': action,
            'response': response,
        }

    def copy(self):
        return AfterHook(
            self.name,
            self.shortname,
            self._call,
            *self.names
        )

    
    
