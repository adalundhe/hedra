import asyncio
from typing import Any
from hedra.core.graphs.hooks.registry.registry_types import ConditionHook, TransformHook, EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes
from .base_event import BaseEvent


class ConditionEvent(BaseEvent[ConditionHook]):

    def __init__(self, target: Hook, source: ConditionHook) -> None:
        super(
            ConditionEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventTypes.CONDITION

    async def call(self, *args, **kwargs): 
        execute_target = await self.execute_pre(*args, **kwargs)

        if execute_target:
            return await self.target.call(*args, **kwargs) 

    async def execute_pre(self, *args, **kwargs) -> bool:

        execute_target = False

        for source in self.pre_sources.values():
            if isinstance(self.target, (EventHook, TransformHook)):
                self.target.conditions.append(source.call)
                execute_target = True
            
            else:
                execute_target = await source.call(*args, **kwargs)
                assert isinstance(execute_target, bool) is True, "Err. - Condition did not return expected result of boolean."

        return execute_target

    async def execute_post(self, result: Any):
        pass