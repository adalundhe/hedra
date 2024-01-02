from hedra.core_rewrite.engines.client.client_types.common import Timeouts
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from typing import Any, Literal, Optional


class CallArg(BaseModel):
    call_name: StrictStr
    call_id: StrictStr
    arg_name: Optional[StrictStr]=None
    arg_type: Literal["arg", "kwarg"]
    position: Optional[StrictInt]=None
    workflow: StrictStr
    engine: StrictStr
    method: StrictStr
    value: Any
    data_type: Literal["static", "dynamic"]
    timeouts: Optional[Timeouts]=None

    class Config:
        arbitrary_types_allowed=True