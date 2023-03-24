from typing import Tuple
from pydantic import BaseModel, validator, StrictStr, StrictInt


class CheckHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    message: StrictStr
    order: StrictInt

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))
