import asyncio
from typing import Any, Optional
from hedra.core.graphs.hooks.registry.registry_types import TransformHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
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
        
        self.event_type = EventType.TRANSFORM
