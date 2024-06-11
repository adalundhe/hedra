from typing import Callable, Optional

from playwright.async_api import ConsoleMessage
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectConsoleMessageCommand(BaseModel):
    predicate: Optional[
        Callable[
            [ConsoleMessage],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True