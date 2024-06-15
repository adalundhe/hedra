from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class PressCommand(BaseModel):
    key: StrictStr
    delay: StrictInt | StrictFloat
    timeout: StrictInt | StrictFloat