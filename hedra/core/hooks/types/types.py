from enum import Enum


class HookType(Enum):
    ACTION='ACTION'
    SETUP='SETUP'
    TEARDOWN='TEARDOWN'
    BEFORE='BEFORE'
    AFTER='AFTER'
    CHECK='CHECK'
    METRIC='METRIC'
    VALIDATE='VALIDATE'
