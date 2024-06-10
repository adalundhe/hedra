from pydantic import (
    BaseModel,
    StrictStr
)

class URLMetadata(BaseModel):
    host: StrictStr
    path: StrictStr