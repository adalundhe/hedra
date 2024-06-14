from pathlib import Path
from typing import Optional

from playwright.async_api import PdfMargins
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class PdfCommand(BaseModel):
    scale: Optional[StrictFloat] = None
    display_header_footer: Optional[StrictBool] = None
    header_template: Optional[StrictStr]= None
    footer_template: Optional[StrictStr] = None
    print_background: Optional[StrictBool] = None
    landscape: Optional[StrictBool] = None
    page_ranges: Optional[StrictStr] = None
    pdf_format: Optional[StrictStr] = None
    width: Optional[StrictStr | StrictFloat] = None
    height: Optional[StrictStr | StrictFloat] = None
    prefer_css_page_size: Optional[StrictBool] = None
    margin: Optional[PdfMargins] = None
    path: Optional[StrictStr | Path] = None
    outline: Optional[StrictBool] = None
    tagged: Optional[StrictBool] = None
    timeout: Optional[StrictInt | StrictFloat]=None
    