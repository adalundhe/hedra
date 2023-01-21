from typing import Tuple
from pydantic import BaseModel, validator


class AfterValidator(BaseModel):
    names: Tuple[str, ...]

    def __init__(__pydantic_self__, *names: Tuple[str, ...]) -> None:
        super().__init__(names=names)

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))