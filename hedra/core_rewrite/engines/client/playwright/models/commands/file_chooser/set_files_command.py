from pathlib import Path
from typing import Optional, Sequence

from playwright.async_api import FilePayload
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class SetFilesCommand(BaseModel):
    files: StrictStr | Path | FilePayload | Sequence[StrictStr | Path] | Sequence[FilePayload]
    no_wait_after: Optional[bool]=None
    timeout: Optional[StrictInt | StrictFloat]=None