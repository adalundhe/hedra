from typing import Any, Callable, Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class ExposeBindingCommand(BaseModel):
    name: StrictStr
    callback: Callable[..., Any]
    handle: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat
    