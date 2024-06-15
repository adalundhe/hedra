
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class GetAttributeCommand(BaseModel):
    name: StrictStr
    timeout: StrictInt | StrictFloat