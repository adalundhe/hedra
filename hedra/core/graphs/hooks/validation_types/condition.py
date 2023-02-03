from typing import Tuple, Optional, Callable
from pydantic import BaseModel, validator, StrictStr, StrictInt


class EventHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    order: StrictInt

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class ConditionValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        order: int=1
    ) -> None:
        EventHookValidator(
            names=names,
            order=order
        )
