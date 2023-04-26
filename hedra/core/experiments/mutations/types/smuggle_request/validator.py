from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictBytes, 
    Json, 
    StrictInt,
    validator
)

from typing import Optional, Union


class SmuggleRequestValidator(BaseModel):
    request_size: Optional[StrictInt]
    smuggled_request: Optional[Union[StrictStr, StrictBytes, Json]]

    @validator('request_size')
    def validate_request_size(cls, val):

        if val is None:
            assert cls.smuggled_request is not None, "Field smuggled_request cannot be None if request_size is None"

        return val

    @validator('smuggled_request')
    def validate_smuggled_request(cls, val):

        if val is None:
            assert cls.request_size is not None, "Field request_size cannot be None if smuggled_request is None"

        return val