from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictBytes
)
from typing import Union


class InjectHeaderValidator(BaseModel):
    header_name: StrictStr
    header_value: Union[StrictStr, StrictBytes, bytearray]

    class Config:
        arbitrary_types_allowed=True