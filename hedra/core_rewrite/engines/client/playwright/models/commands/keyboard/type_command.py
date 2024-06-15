from typing import Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class TypeCommand(BaseModel):
    text: StrictStr
    delay: Optional[StrictInt | StrictFloat]=None
    timeout: StrictInt | StrictFloat