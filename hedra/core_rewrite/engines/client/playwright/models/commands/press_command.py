from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class PressCommand(BaseModel):
    selector: StrictStr
    key: StrictStr
    delay: Optional[StrictInt | StrictFloat]=None
    no_wait_after: Optional[StrictBool] = None
    strict: Optional[StrictBool]= None
    timeout: StrictInt | StrictFloat = None