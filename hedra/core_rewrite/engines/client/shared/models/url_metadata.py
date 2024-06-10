from typing import Optional

from pydantic import BaseModel, StrictStr


class URLMetadata(BaseModel):
    host: StrictStr
    path: StrictStr
    params: Optional[StrictStr]=None
    query: Optional[StrictStr]=None