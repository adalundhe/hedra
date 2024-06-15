from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class TapCommand(BaseModel):
    x_position: StrictInt | StrictFloat
    y_position: StrictInt | StrictFloat
    timeout: StrictInt | StrictFloat