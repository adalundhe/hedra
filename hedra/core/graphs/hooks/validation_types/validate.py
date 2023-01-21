from typing import Tuple, Optional
from pydantic import BaseModel, Field, validator


class ValidateValidator(BaseModel):
    stage: str=Field(..., min_length=1)
    names: Optional[Tuple[str, ...]]

    def __init__(__pydantic_self__, stage: str, names: Optional[Tuple[str, ...]]=()) -> None:
        super().__init__(
            stage=stage,
            names=names
        )
    
    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals)), "Names must be unique for @validate() hook."