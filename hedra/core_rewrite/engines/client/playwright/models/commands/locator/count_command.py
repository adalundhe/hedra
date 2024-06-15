from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class CountCommand(BaseModel):
    timeout: StrictInt | StrictFloat