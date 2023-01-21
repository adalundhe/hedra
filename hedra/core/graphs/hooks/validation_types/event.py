from typing import Tuple
from pydantic import BaseModel, validator


class EventValidator(BaseModel):
    names: Tuple[str, ...]
    pre: bool=False

    def __init__(__pydantic_self__, *names: Tuple[str, ...], pre: bool=False) -> None:
        super().__init__(
            names=names,
            pre=pre
        )

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))