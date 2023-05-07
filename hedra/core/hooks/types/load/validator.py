from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    Field, 
    StrictStr, 
    StrictInt,
    StrictBool
)


class LoadHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    load_path: StrictStr=Field(..., min_length=1)
    order: StrictInt
    skip: StrictBool

