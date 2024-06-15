
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class WheelCommand(BaseModel):
    delta_x: StrictInt | StrictFloat
    delta_y: StrictInt | StrictFloat
    timeout: StrictInt | StrictFloat
    