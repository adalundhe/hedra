from typing import Literal, Optional, Sequence

from playwright.async_api import Position
from pydantic import (
        BaseModel,
        StrictBool,
        StrictFloat,
        StrictInt,
)


class TapCommand(BaseModel):
        modifiers: Optional[
            Sequence[
                Literal[
                    'Alt', 
                    'Control', 
                    'ControlOrMeta', 
                    'Meta', 
                    'Shift'
                ]
            ]
        ] = None
        position: Optional[Position] = None
        force: Optional[StrictBool] = None
        no_wait_after: Optional[StrictBool] = None
        trial: Optional[StrictBool] = None
        timeout: Optional[StrictInt | StrictFloat]=None