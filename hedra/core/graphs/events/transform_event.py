import asyncio
from typing import Any, Optional
from hedra.core.graphs.hooks.registry.registry_types import TransformHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes
from .base_event import BaseEvent


class TransformEvent(BaseEvent[TransformHook]):

    def __init__(self, target: Hook, source: TransformHook) -> None:
        super(
            TransformEvent,
            self
        ).__init__(
            target,
            source
        )
        
        self.event_type = EventTypes.TRANSFORM

    async def call(self, *args, **kwargs): 
        result = None
        
        result = await self.execute_pre(result)
        result = await self.target.call(*args, **kwargs) 
        result = await self.execute_post(result)

        return result

    async def execute_pre(self, result: Optional[Any]):

        for source in sorted(self.post_sources.values(), key=lambda source: source.order):

            if source.load and source.context:
                result = source.context[source.load]

            result = await source.call(result)
        
            if source.store and result is not None:
                self.target.stage_instance.context[source.store] = result
                source.stage_instance.context[source.store] = result
        
        return result

    async def execute_post(self, result: Optional[Any]):
        for source in sorted(self.post_sources.values(), key=lambda source: source.order):

            if source.load and source.context and result is None:
                result = source.context[source.load]
    
            result = await source.call(result)
        
            if source.store and result is not None:
                self.target.stage_instance.context[source.store] = result
                source.stage_instance.context[source.store] = result
        
        return result