from typing import Optional, Tuple
from pydantic import BaseModel, StrictStr, StrictInt


class ContextHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    order: StrictInt
