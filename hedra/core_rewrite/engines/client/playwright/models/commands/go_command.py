from typing import Literal, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class GoCommand(BaseModel):
    wait_until:  Optional[
        Literal[
            'commit', 
            'domcontentloaded', 
            'load', 
            'networkidle'
        ]
    ] = None
    timeout: StrictInt | StrictFloat
