from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class PressSequentiallyCommand(BaseModel):
    key: StrictStr
    delay: Optional[StrictInt | StrictFloat]=None
    no_wait_after: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat = None