import inspect
from typing import Any, Callable, Awaitable, Optional, Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook


class ContextHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Callable[..., Awaitable[Any]], 
        *names:  Optional[Tuple[str, ...]],
        store: Optional[str]=None,
        load: Optional[str]=None,
        pre: bool=False,
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CONTEXT
        )
        
        self.names = list(set(names))
        self.store = store
        self.load = load
        self.pre = pre
        self.order = order

        self.args = inspect.signature(call)
        self.params = self.args.parameters

    async def call(self, **kwargs) -> None:

        hook_args = {name: value for name, value in kwargs.items() if name in self.params}

        hook_args = hook_args
  
        for param_name in self.params.keys():

            context_value = self.context[param_name]

            if param_name != 'self' and context_value is not None:
                hook_args[param_name] = context_value
        
        if 'results' in list(self.params.keys()):
            results = []

            for stage_results in context_value.values():
                results.extend(
                    stage_results.results
                )

            context_value['results'] = results
        
        context_result = await self._call(**{name: value for name, value in hook_args.items() if name in self.params})

        if isinstance(context_result, dict):

            for context_key, value in context_result.items():
                self.context[context_key] = value

            return {
                **kwargs,
                **context_result
            }

        self.context[self.name] = context_result
        return {
            **kwargs,
            self.shortname: context_result
        }

    def copy(self):
        context_hook = ContextHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            store=self.store,
            load=self.load,
            pre=self.pre,
            order=self.order
        )

        context_hook.stage = self.stage

        return context_hook