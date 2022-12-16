import functools
import json
import os
import psutil
import asyncio
from datetime import datetime
from typing import TextIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .stage import Stage


class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()
        self.data = {}
        self.previous_stage = None
        self.accepted_hook_types = [ HookType.SAVE ]
        self._save_file: TextIO = None
        self.requires_shutdown = True

    @Internal()
    async def run(self):
        
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        timestamp = datetime.now().timestamp()

        save_hooks = self.hooks.get(HookType.SAVE)

        await self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Executing checkpoints for - {len(save_hooks)} - items')
        
        for save_hook in save_hooks:
            checkpoint_data = await save_hook.call(self.data)

            await self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Executing checkpoint - {save_hook.name}')

            if save_hook.config.path is None:

                save_hook.config.path = f'{os.getcwd()}/{self.previous_stage}_{timestamp}.json'

            if os.path.exists(save_hook.config.path):

                checkpoint_filename = Path(save_hook.config.path).stem
                path_dir = str(Path(save_hook.config.path).parent.resolve())

                save_hook.config.path = f'{path_dir}/{checkpoint_filename}_{timestamp}.json'

            self._save_file = open(save_hook.config.path, 'w')

            await loop.run_in_executor(
                executor,
                functools.partial(
                    json.dump,
                    checkpoint_data,
                    self._save_file,
                    indent=4
                )
            )

            await loop.run_in_executor(
                self._executor,
                self._save_file.close
            )

            await self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Checkpoint - {save_hook.name} - complete')

        await self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Completed checkpoints for - {len(save_hooks)} - items')
        

        self._shutdown_task = loop.run_in_executor(None, executor.shutdown)
        

