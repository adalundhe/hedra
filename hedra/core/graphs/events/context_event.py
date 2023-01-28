import asyncio
from typing import List, Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import ContextHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes
from .base_event import BaseEvent


class ContextEvent(BaseEvent[ContextHook]):

    def __init__(self, target: Hook, source: ContextHook) -> None:

        super(
            ContextEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventTypes.CONTEXT

    