from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class BoundingBoxCommand(BaseModel):
    timeout: StrictInt | StrictFloat
    