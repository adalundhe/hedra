from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class KeyCommand(BaseModel):
    key: StrictStr
    timeout: StrictInt | StrictFloat