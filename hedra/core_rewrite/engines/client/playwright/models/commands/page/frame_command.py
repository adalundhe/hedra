from typing import Callable, Optional, Pattern

from pydantic import (
    BaseModel,
    StrictBool,
    StrictStr,
)


class FrameCommand(BaseModel):
    name: Optional[StrictStr]=None
    url: Optional[StrictStr | Pattern[str] | Callable[[StrictStr], StrictBool]]=None
