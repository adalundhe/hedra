import inspect
from typing import Any, Callable, Awaitable, Optional, Tuple
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


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
        self.context: SimpleContext = None
        self.store = store
        self.load = load
        self.pre = pre
        self.order = order

        self.args = inspect.signature(call)
        self.params = self.args.parameters

    async def call(self, **kwargs) -> None:

        hook_args = {name: value for name, value in kwargs.items() if name in self.params}

        execute = await self._execute_call(**hook_args)
        if execute:
            context_value = self.context
            hook_args = hook_args
            if self.load:
                hook_args[self.load] = self.context[self.load]
                
            if self.load == 'results':
                results = []

                for stage_results in context_value.values():
                    results.extend(
                        stage_results.results
                    )

                context_value = results
            
            result = await self._call(**{name: value for name, value in hook_args.items() if name in self.params})
 
            if self.store:
                self.context[self.store] = result

            if isinstance(result, dict):
                return {
                    **kwargs,
                    **result
                }

            return {
                **kwargs,
                'context': result
            }