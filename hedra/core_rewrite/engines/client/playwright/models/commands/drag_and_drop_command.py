from typing import Optional

from playwright.async_api import Position
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class DragAndDropCommand(BaseModel):
    source: StrictStr
    target: StrictStr
    source_position: Optional[Position]=None
    target_position: Optional[Position]=None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    strict: Optional[StrictBool]=None
    trial: Optional[StrictBool]=None