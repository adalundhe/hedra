from typing import Tuple, Optional, Callable
from pydantic import BaseModel, validator, StrictStr, StrictBool


class EventHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class ConditionValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        pre: bool=False, 
        key: Optional[str]=None
    ) -> None:
        EventHookValidator(
            names=names
        )
