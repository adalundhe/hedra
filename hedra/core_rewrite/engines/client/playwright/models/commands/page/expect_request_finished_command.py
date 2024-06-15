from typing import Callable, Optional

from playwright.async_api import Request
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectRequestFinishedCommand(BaseModel):
    predicate: Optional[
        Callable[
            [Request],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True