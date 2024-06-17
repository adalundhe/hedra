from typing import Any, Literal, Optional

from pydantic import BaseModel, StrictInt, StrictStr

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts


class CallArg(BaseModel):
    call_name: StrictStr
    call_id: StrictStr
    arg_name: Optional[StrictStr] = None
    arg_type: Literal["arg", "kwarg"]
    position: Optional[StrictInt] = None
    workflow: StrictStr
    engine: StrictStr
    method: StrictStr
    value: Any
    data_type: Literal["static", "dynamic"]
    timeouts: Optional[Timeouts] = None

    class Config:
        arbitrary_types_allowed = True
