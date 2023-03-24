from collections import defaultdict
from hedra.core.hooks.types.channel.hook import ChannelHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


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

        channel_event.execution_path = list(self.execution_path)
        channel_event.previous_map = list(self.previous_map)
        channel_event.next_map = list(self.next_map)
        channel_event.next_args = defaultdict(dict)

        return channel_event