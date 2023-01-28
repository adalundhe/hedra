import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import ConditionHook
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

    async def _execute(self, event: BaseEvent, *args: Tuple[Any, ...]):

        for sources in self.next_map.values():
            for source_name in sources:
                source = self.events.get(source_name)
                source.conditions.append(source.source.call)

        return (
            event.event_name,
            None
        )

    async def execute_pre(self, *args, **kwargs) -> bool:

        execute_target = False

        if isinstance(self.target, (Hook)):
            for sources in self.next_map.values():
                for source_name in sources:
                    source = self.events.get(source_name)
                    source.conditions.append(source.call)
            
        else:
            execute_target = await source.call(*args, **kwargs)
            assert isinstance(execute_target, bool) is True, "Err. - Condition did not return expected result of boolean."

        return execute_target

    async def execute_post(self, result: Any):
        pass