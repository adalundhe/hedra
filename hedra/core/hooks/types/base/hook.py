import uuid
import inspect
from typing import (
    List, 
    Callable, 
    Any, 
    Awaitable
)
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.hooks.types.base.hook_type import HookType


class Hook:

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Callable[..., Awaitable[Any]], 
        stage: str = None,
        order: int=None,
        skip: bool=False,
        hook_type: HookType=None
    ) -> None:
        self.hook_id = str(uuid.uuid4())
        self.name = name
        self.shortname = shortname

        self._call: Callable[..., Awaitable[Any]] = call
        self.stage = stage
        self.hook_type = hook_type
        self.stage_instance: Any = None
        self.conditions: List[Callable[..., Any]] = []
        self.args = inspect.signature(call)
        self.params = self.args.parameters
        self.context: SimpleContext = SimpleContext()
        self.order: int = order
        self.skip = skip
        

    async def call(self, **kwargs):

        hook_args = {name: value for name, value in kwargs.items() if name in self.params}
        execute = await self._execute_call(**hook_args)

        if execute:
            result = await self._call(**hook_args)

            if isinstance(result, dict):
                return result

            return {
                **kwargs,
                self.name: result
            }


    async def _execute_call(self, **hook_args):
        execute = True
        for condition in self.conditions:
            execute = await condition(**{name: value for name, value in hook_args.items() if name in self.params})

        return execute

    def copy(self):
        return Hook(
            self.name,
            self.shortname,
            self._call,
            stage=self.stage,
            order=self.order,
            skip=self.skip,
            hook_type=self.hook_type
        )
