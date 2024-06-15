from typing import Callable, Optional

from playwright.async_api import Download
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectDownloadCommand(BaseModel):
    predicate: Optional[
        Callable[
            [Download],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True