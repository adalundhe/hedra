from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class GetAttributeCommand(BaseModel):
    selector: StrictStr
    name: StrictStr
    strict: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat