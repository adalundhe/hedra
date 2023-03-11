from typing import Coroutine, List, Type, Callable, Awaitable, Any
from hedra.core.hooks.types.base.hook_type import HookType
from ..base.hook import Hook


class CheckHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: List[str],
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CHECK
        )

        self.names = list(set(names))
        self.order = order

    async def call(self, **kwargs):
        passed = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(passed, dict):
            return {
                **kwargs,
                **passed
            }

        return {
            **kwargs,
            self.shortname: passed
        }

    def copy(self):
        check_hook = CheckHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            oreder=self.order
        )

        check_hook.stage = self.stage

        return check_hook