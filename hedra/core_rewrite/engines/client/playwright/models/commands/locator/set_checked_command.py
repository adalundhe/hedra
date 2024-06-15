from typing import Optional

from playwright.async_api import Position
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class SetCheckedCommand(BaseModel):
    checked: StrictBool
    position: Optional[Position]=None
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    trial: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat
