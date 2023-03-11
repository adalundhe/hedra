from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    validator, 
    StrictStr, 
    StrictInt
)


class TransformHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    order: Optional[StrictInt]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))
