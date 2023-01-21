from typing import Tuple
from pydantic import BaseModel, validator, StrictStr


class BeforeHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))
    

class BeforeValidator:

    def __init__(__pydantic_self__, *names: Tuple[str, ...]) -> None:
        BeforeHookValidator(
            names=names
        )
