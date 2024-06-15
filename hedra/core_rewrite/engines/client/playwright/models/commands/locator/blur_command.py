from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class BlurCommand(BaseModel):
    timeout: StrictInt | StrictFloat