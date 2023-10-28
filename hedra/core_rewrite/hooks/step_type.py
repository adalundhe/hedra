from enum import Enum
from typing import (
    Any,
    TypeVar
)
from hedra.core.engines.types.common.base_result import BaseResult


class StepType(Enum):
    WORK='WORK'
    TEST='TEST'
    CHECK='CHECK'


T = TypeVar('T')


def map_step_type(step_return_type: T) -> StepType:

    if issubclass(step_return_type, BaseResult):
        return StepType.TEST
    
    elif issubclass(step_return_type, Exception):
        return StepType.CHECK
    
    else:
        return StepType.WORK