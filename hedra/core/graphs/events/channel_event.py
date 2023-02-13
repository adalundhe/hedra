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

    def copy(self):
        channel_event = ChannelEvent(
            self.target.copy(),
            self.source.copy()
        )

        channel_event.execution_path = self.execution_path
        channel_event.previous_map = self.previous_map
        channel_event.next_map = self.next_map
        channel_event.next_args = self.next_args

        return channel_event