from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBytes
)

from typing import List, Optional


class ResultValidator(BaseModel):
    error: Optional[StrictStr]
    body: Optional[StrictBytes]
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