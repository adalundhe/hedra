from typing import Tuple
from pydantic import (
    BaseModel, 
    validator, 
    StrictStr, 
    StrictInt, 
    StrictBool
)


class ConditionHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    order: StrictInt
    skip: StrictBool

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))

        return vals

