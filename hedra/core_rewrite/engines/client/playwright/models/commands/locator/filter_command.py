from typing import Optional, Pattern

from playwright.async_api import Locator
from pydantic import (
    BaseModel,
    StrictStr,
)


class FilterCommand(BaseModel):
    has: Optional[Locator]=None
    has_not: Optional[Locator]=None
    has_text: Optional[StrictStr | Pattern]=None
    has_not_text: Optional[StrictStr | Pattern]=None
