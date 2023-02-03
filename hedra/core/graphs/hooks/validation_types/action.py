from typing import List, Optional, Dict, Union, Tuple
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat, validator


class ActionHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    weight: StrictInt
    order: StrictInt
    metadata: Optional[Dict[str, Union[StrictStr, StrictInt, StrictFloat]]]
    checks: Optional[List[StrictStr]]
    notify: Optional[List[StrictStr]]
    listen: Optional[List[StrictStr]]

    class Config:
        arbitrary_types_allowed = True

    @validator('weight', 'order')
    def validate_weight_and_order(cls, val):
        assert val > 0, "Order and weight values must be greater than zero!"


class ActionValidator:

    def __init__(
        self,
        *names: Tuple[str, ...], 
        weight: Optional[int]=1, 
        order: Optional[int]=1, 
        metadata: Optional[Dict[str, Union[str, int, float]]]={}, 
        checks: Optional[List[str]]=[],
        notify: Optional[List[str]]=[],
        listen: Optional[List[str]]=[]
    ) -> None:
        ActionHookValidator(
            names=names,
            weight=weight,
            order=order,
            metadata=metadata,
            checks=checks,
            notify=notify,
            listen=listen
        )
