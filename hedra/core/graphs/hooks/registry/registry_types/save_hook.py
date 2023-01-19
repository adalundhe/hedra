import psutil
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Type, Callable, Awaitable, Any, Dict
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.tools.filesystem import open
from .hook import Hook


class SaveHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: str=None,
        checkpoint_filepath: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.SAVE
        )

        self.context_key = key
        self.save_path = checkpoint_filepath
        self.executor = ThreadPoolExecutor(
            max_workers=psutil.cpu_count(logical=False)
        )
        self.loop = None

    async def call(self, context: SimpleContext) -> None:
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

        return loop.run_until_complete(self._write(context))

    async def _write(self, context: SimpleContext) -> None:

        save_file = await open(self.save_path, 'w')
        context_data = context.get(self.context_key)

        if context_data and self.context_key == 'results':
            results_data: Dict[str, ResultsSet] = context_data
            for stage_name in context_data:
                context_data[stage_name] = results_data[stage_name].to_serializable()

        await save_file.write(
            await self._call(context_data)
        )

        await save_file.close()


        if context.get(self.context_key):
            context[self.context_key] = type(context[self.context_key])()


        

