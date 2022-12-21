import asyncio
from typing import List, Union
from datetime import datetime
from hedra.core.graphs.events import Event
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook, 
    SaveHook,
    RestoreHook,
    ContextHook
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
            HookType.CONTEXT ,
            HookType.EVENT, 
            HookType.RESTORE,
            HookType.SAVE, 
        ]

        self.requires_shutdown = True

    @Internal()
    async def run(self):

        events: List[Union[EventHook, Event]] = [event for event in self.hooks[HookType.EVENT]]
        pre_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre
        ]
        
        if len(pre_events) > 0:
            pre_event_names = ", ".join([
                event.shortname for event in pre_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing PRE events - {pre_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in pre_events
            ], timeout=self.stage_timeout)

        restore_hooks: List[RestoreHook] = self.hooks[HookType.RESTORE]
        restore_paths: List[str] = [restore_hook.restore_path for restore_hook in restore_hooks]
        restore_hooks_count = len(restore_hooks)

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
        
        post_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre is False
        ]

        if len(post_events) > 0:
            post_event_names = ", ".join([
                event.shortname for event in post_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing POST events - {post_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in post_events
            ], timeout=self.stage_timeout)

        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        await asyncio.gather(*[
            asyncio.create_task(context_hook.call(self.context)) for context_hook in context_hooks
        ])

        await self.logger.spinner.set_default_message(f"Checkpoint complete - Restored {restore_hooks_count} items and saved {save_hooks_count} items")
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed checkpoints for - {len(save_hooks)} - items')

