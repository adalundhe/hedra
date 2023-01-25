from typing import Any, Optional
from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes
from .base_event import BaseEvent


class Event(BaseEvent[EventHook]):

    def __init__(self, target: Hook, source: EventHook) -> None:
        super(
            Event,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventTypes.EVENT

    async def call(self, *args, **kwargs): 
        result = None
        
        result = await self.execute_pre(result)
        result = await self.target.call(*args, **kwargs) 
        result = await self.execute_post(result)

        return result

    async def execute_pre(self, result: Optional[Any]):

        for source in self.pre_sources.values():
            result = await source.call(result)

            if source.key:
                source.stage_instance.context[source.key] = result
                self.target.stage_instance.context[source.key] = result

        return result

    async def execute_post(self, result: Any):
        for source in self.post_sources.values():
            source_hook_input = result
            if source.key:
                source_hook_input = self.target.stage_instance.context[source.key]

                if source_hook_input is None:
                    source_hook_input = source.stage_instance.context[source.key]

                if source_hook_input is None:
                    source_hook_input = result

            source_result = await source.call(source_hook_input)

            if source.key:
                source.stage_instance.context[source.key] = source_result
                self.target.stage_instance.context[source.key] = source_result

            else:
                result = source_result
        
        return result