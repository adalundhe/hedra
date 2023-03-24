from typing import List, Callable, Awaitable, Any
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook


class CheckHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: List[str],
        message: str='Did not return True.',
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CHECK
        )

        self.message = message
        self.names = list(set(names))
        self.order = order

    async def call(self, **kwargs):
        passed = await self._call(**{
            name: value for name, value in kwargs.items() if name in self.params
        })

        result = kwargs.get('result')

        if passed is False and result:
            result.error = f'Check - {self.name} - failed. Context - {self.message}'

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