from typing import List, Optional, Dict, Union, Tuple
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat, validator


class TaskHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    weight: StrictInt
    order: StrictInt
    metadata: Optional[Dict[str, Union[StrictStr, StrictInt, StrictFloat]]]

    class Config:
        arbitrary_types_allowed = True

    @validator('weight', 'order')
    def validate_weight_and_order(cls, val):
        assert val > 0

class TaskValidator(BaseModel):

    def __init__(
        self,
        *names: Tuple[str, ...], 
        weight: Optional[int]=1, 
        order: Optional[int]=1, 
        metadata: Optional[Dict[str, Union[str, int, float]]]={}
    ) -> None:
        TaskHookValidator(
            names=names,
            weight=weight,
            order=order,
            metadata=metadata
        )