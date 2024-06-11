from typing import List, Literal, Optional

from playwright.async_api import Geolocation
from pydantic import BaseModel, StrictStr


class BrowserMetadata(BaseModel):
    browser_type: Optional[
        Literal[
            'safari',
            'webkit',
            'firefox',
            'chrome',
            'chromium'
        ]
    ]=None,
    device_type: Optional[StrictStr]=None, 
    locale: Optional[StrictStr]=None
    geolocation: Optional[Geolocation]=None
    permissions: Optional[List[StrictStr]]=None
    color_scheme: Optional[StrictStr]=None

    class Config:
        arbitrary_types_allowed=True
