from typing import Dict, Union
from hedra.core.graphs.hooks.types.base.hook_type import HookType
from hedra.core.graphs.hooks.types.base.hook import Hook
from ..hooks.types.action.event import ActionEvent
from ..hooks.types.channel.event import ChannelEvent
from ..hooks.types.check.event import CheckEvent
from ..hooks.types.condition.event import ConditionEvent
from ..hooks.types.context.event import ContextEvent
from ..hooks.types.event.event import Event
from ..hooks.types.load.event import LoadEvent
from ..hooks.types.save.event import SaveEvent
from ..hooks.types.task.event import TaskEvent
from ..hooks.types.transform.event import TransformEvent


HedraEvent = Union[Event, TransformEvent, ConditionEvent, ContextEvent, SaveEvent, LoadEvent, CheckEvent]


def get_event(target: Hook, source: Hook) -> HedraEvent:
    event_types: Dict[HookType, HedraEvent] = {
        HookType.ACTION: ActionEvent,
        HookType.CHANNEL: ChannelEvent,
        HookType.CHECK: CheckEvent,
        HookType.CONDITION: ConditionEvent,
        HookType.CONTEXT: ContextEvent,
        HookType.EVENT: Event,
        HookType.LOAD: LoadEvent,
        HookType.SAVE: SaveEvent,
        HookType.TASK: TaskEvent,
        HookType.TRANSFORM: TransformEvent
    }

    return event_types.get(
        source.hook_type, 
        Event
    )(target, source)