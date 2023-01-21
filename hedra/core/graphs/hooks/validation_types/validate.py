from typing import Tuple, Optional
from pydantic import BaseModel, Field, validator, StrictStr


class ValidateHookValidator(BaseModel):
    stage: StrictStr=Field(..., min_length=1)
    names: Optional[Tuple[StrictStr, ...]]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals)), "Names must be unique for @validate() hook."


class ValidateValidator:

    def __init__(__pydantic_self__, stage: str, names: Optional[Tuple[str, ...]]=()) -> None:
        ValidateHookValidator(
            stage=stage,
            names=names
        )
    