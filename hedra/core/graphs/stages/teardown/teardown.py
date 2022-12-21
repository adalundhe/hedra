import asyncio
import inspect
from typing import List, Union
from hedra.core.graphs.events import Event
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook,
    ContextHook,
    TeardownHook
)
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []
        self.accepted_hook_types = [ HookType.EVENT, HookType.CONTEXT ]

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

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing PRE events - {pre_event_names}')
        await asyncio.wait([asyncio.create_task(event.call()) for event in pre_events], timeout=self.stage_timeout)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting Teardown stage.')

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)

        teardown_hooks: List[TeardownHook] = self.hooks[HookType.TEARDOWN]

        if teardown_hooks:

            teardown_hook_names = ', '.join([hook.name for hook in teardown_hooks])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Running teardown hooks - {teardown_hook_names}')           
            await asyncio.gather(*[ hook.call() for hook in teardown_hooks])

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
        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        await asyncio.gather(*[
            asyncio.create_task(context_hook.call(self.context)) for context_hook in context_hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Teardown complete.')
