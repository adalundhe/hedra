from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class FrameElementCommand(BaseModel):
    timeout: StrictInt | StrictFloat