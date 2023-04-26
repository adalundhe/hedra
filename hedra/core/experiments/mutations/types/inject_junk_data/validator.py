from pydantic import (
    BaseModel, 
    StrictInt,
    StrictStr
)
from typing import Optional


class InjectJunkDataValidator(BaseModel):
    junk_size: StrictInt
