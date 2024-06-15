from typing import Literal, Optional, Sequence

from playwright.async_api import Position
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class HoverCommand(BaseModel):
    modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None
    postion: Optional[Position]=None
    timeout: StrictInt | StrictFloat
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    trial: Optional[StrictBool]=None
    
    class Config:
        arbitrary_types_allowed=True
        