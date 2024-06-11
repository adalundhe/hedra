from typing import Callable, Optional, Pattern

from playwright.async_api import Request
from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class ExpectRequestCommand(BaseModel):
    url_or_predicate: Optional[
        StrictStr |
        Pattern[StrictStr] |
        Callable[
            [Request],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True