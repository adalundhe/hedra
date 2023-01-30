from typing import TypeVar, Dict, Union
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registry_types import (
    ConditionHook,
    EventHook,
    TransformHook,
    ContextHook
)
from .condition_event import ConditionEvent
from .context_event import ContextEvent
from .event import Event
from .transform_event import TransformEvent
from .event_dispatch import EventDispatcher


T = TypeVar('T', EventHook, TransformHook, ConditionHook, ContextHook)


HedraEvent = Union[Event, TransformEvent, ConditionEvent, ContextEvent]


def get_event(target: Hook, source: T) -> HedraEvent:
    event_types: Dict[HookType, HedraEvent] = {
        HookType.CONDITION: ConditionEvent,
        HookType.CONTEXT: ContextEvent,
        HookType.EVENT: Event,
        HookType.TRANSFORM: TransformEvent
    }

    return event_types.get(
        source.hook_type, 
        Event
    )(target, source)