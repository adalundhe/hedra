from typing import Literal, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class WaitForLoadStateCommand(BaseModel):
    state: Optional[
        Literal[
            'domcontentloaded', 
            'load', 
            'networkidle'
        ]
    ]=None
    timeout: StrictInt | StrictFloat