from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class SetTimeoutCommand(BaseModel):
    timeout: StrictInt | StrictFloat