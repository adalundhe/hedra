from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat
)

from typing import List, Optional


class WebsocketResultValidator(BaseModel):
    error: Optional[StrictStr]
    status: StrictInt
    reason: Optional[StrictStr]
    params: Optional[StrictStr]
    query: StrictStr
    wait_start: StrictFloat
    start: StrictFloat
    connect_end: StrictFloat
    write_end: StrictFloat
    complete: StrictFloat
    checks: Optional[List[StrictStr]]