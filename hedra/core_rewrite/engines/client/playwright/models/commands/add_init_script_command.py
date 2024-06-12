from pathlib import Path
from typing import Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class AddInitScriptCommand(BaseModel):
    script: Optional[StrictStr]=None
    path: Optional[StrictStr | Path]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True
    