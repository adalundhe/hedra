import psutil
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Awaitable, Any, Tuple, Dict
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.tools.filesystem import open


RawResultsSet = Dict[str, ResultsSet]


class SaveHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        save_path: str=None,
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.SAVE
        )

        self.names = list(set(names))
        self.save_path = save_path
        self.executor = ThreadPoolExecutor(
            max_workers=psutil.cpu_count(logical=False)
        )
        self.order: int = order
        self.loop = None

    async def call(self, **kwargs) -> None:
        self.loop = asyncio.get_event_loop()

        return await self.loop.run_in_executor(
            self.executor,
            functools.partial(
                self._run,
                **kwargs
            )
        )

    def _run(self, **kwargs):
        import asyncio
        import uvloop
        uvloop.install()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        return loop.run_until_complete(self._write(**kwargs))

    async def _write(self, **kwargs) -> None:

        save_file = await open(self.save_path, 'w')

        hook_args = {name: value for name, value in kwargs.items() if name in self.params}

        for param_name in self.params.keys():

            if param_name == 'results':
                
                results: RawResultsSet = self.context[param_name]
                
                for stage_name, result, in results.items():
                    results[stage_name] = result.to_serializable()

                context_value = results

            else:

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

        await save_file.write(
            await self._call(**{name: value for name, value in kwargs.items() if name in self.params})
        )

        await save_file.close()

        for context_key in hook_args:
            if self.context.get(context_key):
                self.context[context_key] = type(self.context[context_key])()
                kwargs[context_key] = type(self.context[context_key])()

        return kwargs    

    def copy(self):
        save_hook = SaveHook(
            self.name,
            self.shortname,
            self._call,
            self.save_path,
            order=self.order
        )

        save_hook.stage = self.stage

        return save_hook