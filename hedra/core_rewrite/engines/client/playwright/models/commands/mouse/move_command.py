from typing import Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class MoveCommand(BaseModel):
    x_position: StrictInt | StrictFloat
    y_position: StrictInt | StrictFloat
    steps: Optional[StrictInt]=1
    timeout: StrictInt | StrictFloat
    