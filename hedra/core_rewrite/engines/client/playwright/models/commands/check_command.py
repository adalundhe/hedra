from typing import Optional

from playwright.async_api import Position
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class CheckCommand(BaseModel):
    selector: StrictStr
    postion: Optional[Position]=None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    strict: Optional[StrictBool]=None
    trial: Optional[StrictBool]=None
    
    class Config:
        arbitrary_types_allowed=True
        