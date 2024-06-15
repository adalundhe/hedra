from typing import Any, Dict, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class DispatchEventCommand(BaseModel):
    event_type: StrictStr
    event_init: Optional[Dict[StrictStr, Any]]=None
    timeout: StrictInt | StrictFloat=None
