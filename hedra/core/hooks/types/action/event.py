from collections import defaultdict
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


class ActionEvent(BaseEvent[ActionHook]):

    def __init__(self, target: Hook, source: ActionHook) -> None:
        super(
            ActionEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.ACTION

    def copy(self):
        action_event = ActionEvent(
            self.target.copy(),
            self.source.copy()
        )
        
        action_event.execution_path = list(self.execution_path)
        action_event.previous_map = list(self.previous_map)
        action_event.next_map = list(self.next_map)
        action_event.next_args = defaultdict(dict)

        return action_event
