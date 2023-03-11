from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    Field, 
    StrictStr, 
    StrictInt
)


class SaveHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    save_path: StrictStr=Field(..., min_length=1)
    order: StrictInt
