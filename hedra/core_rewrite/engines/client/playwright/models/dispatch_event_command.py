from typing import Any, Dict, Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class DispatchEventCommand(BaseModel):
    selector: StrictStr
    event_type: StrictStr
    event_init: Optional[Dict[StrictStr, Any]]=None
    strict: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat=None
