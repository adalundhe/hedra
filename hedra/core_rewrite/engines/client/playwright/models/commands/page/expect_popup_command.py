from typing import Callable, Optional

from playwright.async_api import Page
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ExpectPopupCommand(BaseModel):
    predicate: Optional[
        Callable[
            [Page],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True