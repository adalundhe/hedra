from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class OpenerCommand(BaseModel):
    timeout: StrictInt | StrictFloat