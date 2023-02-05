from typing import Tuple
from pydantic import BaseModel, validator, StrictStr, StrictInt


class CheckHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    order: StrictInt

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))


class CheckValidator:
    names: Tuple[str, ...]

    def __init__(__pydantic_self__, *names: Tuple[str, ...], order: int=1) -> None:
        CheckHookValidator(
            names=names,
            order=order
        )