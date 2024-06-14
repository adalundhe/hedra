from playwright.async_api import Locator
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class RemoveLocatorHandlerCommand(BaseModel):
    locator: Locator
    timeout: StrictInt | StrictFloat