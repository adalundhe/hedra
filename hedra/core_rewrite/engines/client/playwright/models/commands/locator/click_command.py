from typing import Literal, Optional, Sequence

from playwright.async_api import Position
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class ClickCommand(BaseModel):
    modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None
    delay: Optional[StrictFloat | StrictInt]=None
    button: Optional[Literal['left', 'middle', 'right']]=None
    click_count: Optional[StrictInt]=None
    postion: Optional[Position]=None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    trial: Optional[StrictBool]=None

    class Config:
        arbitrary_types_allowed=True
        