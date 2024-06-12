from pathlib import Path
from typing import Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class AddStyleTagCommand(BaseModel):
    url: Optional[StrictStr]=None
    path: Optional[StrictStr | Path]=None
    content: Optional[StrictStr]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True
    