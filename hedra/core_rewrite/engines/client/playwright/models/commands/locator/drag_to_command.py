from typing import Optional

from playwright.async_api import (
    Locator,
    Position,
)
from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt


class DragToCommand(BaseModel):
    target: Locator
    force: Optional[StrictBool] = None
    no_wait_after: Optional[StrictBool] = None
    trial: Optional[StrictBool] = None
    source_position: Optional[Position] = None
    target_position: Optional[Position] = None
    timeout: Optional[StrictInt | StrictFloat] =None
