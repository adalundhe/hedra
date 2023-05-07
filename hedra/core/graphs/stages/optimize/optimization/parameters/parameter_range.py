from typing import Union
from pydantic import (
    BaseModel, 
    StrictInt, 
    StrictFloat, 
    validator
)


class ParameterRange(BaseModel):
    minimum_range: Union[StrictInt, StrictFloat]
    maximum_range: Union[StrictInt, StrictFloat]

    class Config:
        arbitrary_types_allowed = True

    @validator('minimum_range', 'maximum_range')
    def validate_param_range(cls, val):
        assert val > 0, "Order and weight values must be greater than zero!"

        return val
