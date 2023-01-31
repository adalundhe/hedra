import asyncio
from typing import List
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.registry.registry_types import (
    SaveHook,
    RestoreHook
)
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage


class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()
        self.previous_stage = None
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.RESTORE,
            HookType.SAVE, 
            HookType.TRANSFORM
        ]

        self.requires_shutdown = True

    @Internal()
    async def run(self):
        await self.setup_events()
        await self.dispatcher.dispatch_events()

    @event()
    async def setup_restore_config(self):

        restore_hooks: List[RestoreHook] = self.hooks[HookType.RESTORE]
        restore_paths: List[str] = [restore_hook.restore_path for restore_hook in restore_hooks]
        restore_hooks_count = len(restore_hooks)

        return {
            'restore_hooks': restore_hooks,
            'restore_paths': restore_paths,
            'restore_hooks_count': restore_hooks_count
        }

    @event('setup_restore_config')
    async def restore_to_context(
        self,
        restore_hooks: List[RestoreHook]=[],
        restore_hooks_count: int=0
    ):
        if restore_hooks_count > 0:
            await self.logger.spinner.append_message(f'Executing Restore checkpoints for - {restore_hooks_count} - items')
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing Restore checkpoints for - {restore_hooks_count} - items')

        for restore_hook in restore_hooks:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Restore Hook {restore_hook.name}:{restore_hook.hook_id} - Restoring data from checkpoint file - {restore_hook.restore_path}')

        await asyncio.gather(*[
            asyncio.create_task(
                restore_hook.call(self.context)
            ) for restore_hook in restore_hooks
        ])
        

    @event('setup_restore_config')
    async def save_from_context(
        self,
        restore_paths: List[str]=[]
    ):

        save_hooks: List[SaveHook] = self.hooks[HookType.SAVE]
        allowed_save_hooks = [save_hook for save_hook in save_hooks if save_hook.save_path not in restore_paths]
        save_hooks_count = len(allowed_save_hooks)
        
        if save_hooks_count:
            await self.logger.spinner.append_message(f'Executing Save checkpoints for - {save_hooks_count} - items')
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing Save checkpoints for - {len(save_hooks)} - items')

        
        for save_hook in allowed_save_hooks:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Save Hook {save_hook.name}:{save_hook.hook_id} - Saving data to checkpoint file - {save_hook.save_path}')
        
        await asyncio.gather(*[
            asyncio.create_task(
                save_hook.call(self.context)
            ) for save_hook in allowed_save_hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed checkpoints for - {len(save_hooks)} - items')

        return {
            'save_hooks_count': save_hooks_count
        }
    
    @event('restore_to_context','save_from_context')
    async def complete_checkpoint(
        self,
        restore_hooks_count: int=0,
        save_hooks_count: int=0
    ):

        await self.logger.spinner.set_default_message(f"Checkpoint complete - Restored {restore_hooks_count} items and saved {save_hooks_count} items")

