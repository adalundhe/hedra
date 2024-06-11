from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class FillCommand(BaseModel):
    selector: StrictStr
    value: StrictStr
    no_wait_after: Optional[StrictBool]=None
    strict: Optional[StrictBool]=None
    force: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat