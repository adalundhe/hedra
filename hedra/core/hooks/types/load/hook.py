import psutil
import asyncio
import re
import functools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Awaitable, Any, Dict, Union, List, Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.tools.filesystem import open


RawResultsSet = Dict[str, Dict[str, Union[int, float, List[Any]]]]


class LoadHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        load_path: str=None,
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.LOAD
        )

        self.names = list(set(names))
        self.load_path = load_path
        self.executor = ThreadPoolExecutor(
            max_workers=psutil.cpu_count(logical=False)
        )
        self.loop = None
        self.order = order
        self._strip_pattern = re.compile('[^a-z]+'); 

    async def call(self, **kwargs) -> None:

        execute = await self._execute_call(**kwargs)

        if execute:
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

        return loop.run_until_complete(self._load(**kwargs))

    async def _load(self, **kwargs):

        restore_file = await open(self.load_path, 'r')
        
        file_stem = Path(self.load_path).stem
        restore_slug = self._strip_pattern.sub('',file_stem.lower())
        file_data = await restore_file.read()

        hook_args = {name: value for name, value in kwargs.items() if name in self.params}
        hook_args[restore_slug] = file_data

        load_result: Union[Any, dict] = await self._call(**{name: value for name, value in hook_args.items() if name in self.params})

        if restore_slug == 'results':

            for stage_name, stage_data in load_result.items():

                if isinstance(stage_data, ResultsSet) is False:
                    results_set = ResultsSet(stage_data)
                    results_set.load_results()

                    load_result[stage_name] = results_set

        self.context[file_data] = load_result

        await restore_file.close()

        if isinstance(load_result, dict):
            return {
                **kwargs,
                **load_result
            }

        return {
            **kwargs,
            self.shortname: load_result
        }

    def copy(self):
        load_hook = LoadHook(
            self.name,
            self.shortname,
            self._call,
            self.load_path,
            order=self.order
        )

        load_hook.stage = self.stage

        return load_hook