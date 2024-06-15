from typing import Optional, Pattern

from playwright.async_api import Locator
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class LocatorCommand(BaseModel):
    selector: StrictStr
    has_text: Optional[StrictStr | Pattern[StrictStr]]=None
    has_not_text: Optional[StrictStr | Pattern[StrictStr]]=None
    has: Optional[Locator]=None
    has_not: Optional[Locator]=None
    timeout: StrictInt | StrictFloat