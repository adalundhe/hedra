from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class IsClosedCommand(BaseModel):
    timeout: StrictInt | StrictFloat