from typing import TypeVar, Dict, Union
from hedra.core.graphs.hooks.types.base.hook_type import HookType
from hedra.core.graphs.hooks.types.base.hook import Hook
from ..hooks.types.action.event import ActionEvent
from ..hooks.types.channel.event import ChannelEvent
from .check_event import CheckEvent
from .condition_event import ConditionEvent
from .context_event import ContextEvent
from .event import Event
from .load_event import LoadEvent
from .save_event import SaveEvent
from .task_event import TaskEvent
from .transform_event import TransformEvent


T = TypeVar('T')


HedraEvent = Union[Event, TransformEvent, ConditionEvent, ContextEvent, SaveEvent, LoadEvent, CheckEvent]


def get_event(target: Hook, source: T) -> HedraEvent:
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