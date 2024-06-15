from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class DOMCommand(BaseModel):
    selector: StrictStr
    strict: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat