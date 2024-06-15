from typing import (
    Any,
    Callable,
    Optional,
)

from playwright.async_api import Locator
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class AddLocatorHandlerCommand(BaseModel):
    locator: Locator
    handler: Callable[
        [Locator],
        Any
    ] | Callable[[], Any]
    no_wait_after: Optional[StrictBool]=None
    times: Optional[StrictInt]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True