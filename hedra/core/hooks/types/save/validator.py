from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    Field, 
    StrictStr, 
    StrictInt,
    StrictBool
)


class SaveHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    save_path: StrictStr=Field(..., min_length=1)
    order: StrictInt
    skip: StrictBool
