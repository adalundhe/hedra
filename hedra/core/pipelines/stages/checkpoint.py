import functools
import json
import psutil
import asyncio
from typing import TextIO
from concurrent.futures import ThreadPoolExecutor
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()
        self.data = {}
        self.previous_stage = ''
        self.accepted_hook_types = [ HookType.SAVE ]
        self._loop = None
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._save_file: TextIO = None

    @Internal
    async def run(self):
        
        self._loop = asyncio.get_event_loop()
        for save_hook in self.hooks.get(HookType.SAVE):
            checkpoint_data = await save_hook.call(self.data)

            self._save_file = open(save_hook.config.path, 'w')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    json.dump,
                    checkpoint_data,
                    self._save_file,
                    indent=4
                )
            )

            await self._loop.run_in_executor(
                self._executor,
                self._save_file.close
            )

        self._executor.shutdown(wait=True)