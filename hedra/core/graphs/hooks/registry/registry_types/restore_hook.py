import psutil
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Awaitable, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.tools.filesystem import open
from .hook import Hook


class RestoreHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: str=None,
        restore_filepath: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.RESTORE
        )

        self.context_key = key
        self.restore_path = restore_filepath
        self.executor = ThreadPoolExecutor(
            max_workers=psutil.cpu_count(logical=False)
        )
        self.loop = None

    async def call(self, context: SimpleContext) -> None:

        execute = await self._execute_call(context[self.context_key])

        if execute:
            self.loop = asyncio.get_event_loop()
            return await self.loop.run_in_executor(
                self.executor,
                self._run,
                context
            )

    def _run(self, context: SimpleContext):
        import asyncio
        import uvloop
        uvloop.install()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        return loop.run_until_complete(self._load(context))

    async def _load(self, **kwargs,):

        context: SimpleContext = kwargs.get('context')
        restore_file = await open(self.restore_path, 'r')
        file_data = await restore_file.read()

        kwargs[self.context_key] = context[self.context_key]
        kwargs['file'] = file_data

        data = await self._call(**{name: value for name, value in kwargs.items() if name in self.params})

        if self.context_key == 'results':

            for stage_name, stage_data in data.items():

                if isinstance(stage_data, ResultsSet) is False:
                    results_set = ResultsSet(stage_data)
                    results_set.load_results()

                    data[stage_name] = results_set

        context[self.context_key] = data

        await restore_file.close()

        if isinstance(data, dict):
            return {
                **kwargs,
                **data
            }

        return {
            **kwargs,
            self.context_key: data
        }
