from typing import Tuple
from pydantic import BaseModel, validator, StrictStr, StrictBool


class EventHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    pre: StrictBool=False

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))


class EventValidator:

    def __init__(__pydantic_self__, *names: Tuple[str, ...], pre: bool=False) -> None:
        EventHookValidator(
            names=names,
            pre=pre
        )
