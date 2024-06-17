from typing import Any, Callable, Optional, Pattern

from playwright.async_api import Request, Route
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class RouteCommand(BaseModel):
    url: StrictStr | Pattern[str] | Callable[[StrictStr], StrictBool]
    handler: Callable[[Route], Any] | Callable[[Route, Request], Any]
    times: Optional[StrictInt]
    timeout: StrictInt | StrictFloat