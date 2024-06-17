from playwright.async_api import Locator
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class RemoveLocatorHandlerCommand(BaseModel):
    locator: Locator
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True