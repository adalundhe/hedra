from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class GetByTextCommand(BaseModel):
    text: StrictStr
    exact: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat