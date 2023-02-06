import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import ChannelHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class ChannelEvent(BaseEvent[ChannelHook]):

    def __init__(self, target: Hook, source: ChannelHook) -> None:
        super(
            ChannelEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.CHANNEL
