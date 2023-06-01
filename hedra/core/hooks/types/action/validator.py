from typing import Optional, Dict, Union, Tuple, Any
from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictInt, 
    StrictFloat, 
    StrictBool, 
    validator
)


class ActionHookValidator(BaseModel):
    names: Tuple[StrictStr, ...]
    weight: StrictInt
    order: StrictInt
    loader_config: Optional[Dict[StrictStr, Any]]
    metadata: Optional[Dict[str, Union[StrictStr, StrictInt, StrictFloat]]]
    skip: StrictBool

    class Config:
        arbitrary_types_allowed = True

    @validator('weight', 'order')
    def validate_weight_and_order(cls, val):
        assert val > 0, "Order and weight values must be greater than zero!"
        return val


class ActionLoaderConfigValidator(BaseModel):
    action_name: StrictStr
    loader_id: StrictStr
    loader_name: StrictStr
    loader_type: StrictStr
    loader_config: Dict[str, str]
