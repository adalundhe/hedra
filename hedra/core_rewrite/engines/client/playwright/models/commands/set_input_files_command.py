from pathlib import Path
from typing import Optional, Sequence

from playwright.async_api import FilePayload
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class SetInputFilesCommand(BaseModel):
    selector: StrictStr
    files: StrictStr | Path | FilePayload | Sequence[StrictStr | Path] | Sequence[FilePayload]
    strict: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat
    