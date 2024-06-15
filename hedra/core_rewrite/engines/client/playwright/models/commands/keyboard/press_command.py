from typing import Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class PressCommand(BaseModel):
    key: StrictStr
    delay: Optional[StrictInt | StrictFloat]=None
    timeout: StrictInt | StrictFloat