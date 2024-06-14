from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class PauseCommand(BaseModel):
    timeout: StrictInt | StrictFloat