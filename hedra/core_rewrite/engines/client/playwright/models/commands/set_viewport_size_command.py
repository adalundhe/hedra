from playwright.async_api import ViewportSize
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class SetViewportSize(BaseModel):
    viewport_size: ViewportSize
    timeout: StrictInt | StrictFloat