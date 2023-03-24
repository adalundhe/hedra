from typing import Tuple
from pydantic import BaseModel, validator, StrictStr, StrictInt


class ConditionHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    order: StrictInt

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))

