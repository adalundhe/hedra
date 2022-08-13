import functools
import json
import os
import psutil
import asyncio
from datetime import datetime
from typing import TextIO
from pathlib import Path
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
        self.previous_stage = None
        self.accepted_hook_types = [ HookType.SAVE ]
        self._loop = None
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._save_file: TextIO = None

    @Internal
    async def run(self):
        
        self._loop = asyncio.get_event_loop()
        timestamp = datetime.now().timestamp()
        
        for save_hook in self.hooks.get(HookType.SAVE):
            checkpoint_data = await save_hook.call(self.data)

            if save_hook.config.path is None:

                save_hook.config.path = f'{os.getcwd()}/{self.previous_stage}_{timestamp}.json'

            if os.path.exists(save_hook.config.path):

                checkpoint_filename = Path(save_hook.config.path).stem
                path_dir = str(Path(save_hook.config.path).parent.resolve())

                save_hook.config.path = f'{path_dir}/{checkpoint_filename}_{timestamp}.json'

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
