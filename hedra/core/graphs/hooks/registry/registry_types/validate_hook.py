from typing import Type, Callable, Awaitable, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ValidateHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: str
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.VALIDATE
        )
        
        self.names = list(set(names))


    async def call(self, **kwargs):
        result = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(result, dict):
            return {
                **kwargs,
                **result
            }

        return {
            **kwargs,
            'valid': result
        }