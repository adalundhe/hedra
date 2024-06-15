from typing import Callable, Optional, Pattern

from pydantic import (
    BaseModel,
    StrictBool,
    StrictStr,
)


class FrameCommand(BaseModel):
    name: Optional[StrictStr]=None
    url: Optional[StrictStr | Pattern[StrictStr] | Callable[[StrictStr], StrictBool]]=None
