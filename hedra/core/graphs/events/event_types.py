from enum import Enum


class EventType(Enum):
    CHECK='CHECK'
    CONDITION='CONDITION'
    CONTEXT='CONTEXT'
    EVENT='EVENT'
    TRANSFORM='TRANSFORM'
    SAVE='SAVE'
    LOAD='LOAD'
    ACTION='ACTION'
    TASK='TASK'
