from enum import Enum


class EventType(Enum):
    CONDITION='CONDITION'
    CONTEXT='CONTEXT'
    EVENT='EVENT'
    TRANSFORM='TRANSFORM'
    SAVE='SAVE'
    LOAD='LOAD'
