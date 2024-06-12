from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class TypeCommand(BaseModel):
    selector: StrictStr
    text: StrictStr
    delay: Optional[StrictInt | StrictFloat]=None
    no_wait_after: Optional[StrictInt | StrictFloat]=None
    strict: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat
    