from pydantic import (
    BaseModel,
    StrictStr
)
from typing import Optional

class Metadata(BaseModel):
    protocol: Optional[StrictStr]