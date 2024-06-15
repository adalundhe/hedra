from pathlib import Path
from typing import Literal, Optional, Sequence

from playwright.async_api import FloatRect, Locator
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class ScreenshotCommand(BaseModel):
    path: StrictStr | Path
    image_type: Literal['jpeg', 'png']='png'
    quality: Optional[StrictInt]= None
    omit_background: Optional[StrictBool] = None
    full_page: Optional[StrictBool] = None
    clip: Optional[FloatRect] = None
    animations: Literal['allow', 'disabled'] = 'allow'
    caret: Literal['hide', 'initial'] = 'hide'
    scale: Literal['css', 'device'] = 'device'
    mask: Optional[Sequence[Locator]] = None
    mask_color: Optional[StrictStr] = None
    style: Optional[StrictStr] = None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True