from typing import Optional, Dict, Union, Tuple
from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictInt, 
    StrictFloat,
    StrictBool, 
    validator
)


class TaskHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    weight: StrictInt
    order: StrictInt
    skip: StrictBool
    metadata: Optional[Dict[str, Union[StrictStr, StrictInt, StrictFloat]]]

    class Config:
        arbitrary_types_allowed = True

    @validator('weight', 'order')
    def validate_weight_and_order(cls, val):
        assert val > 0

        return val
