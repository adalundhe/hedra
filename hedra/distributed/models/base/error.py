from pydantic import BaseModel, StrictStr, StrictInt
from typing import Optional

class Error(BaseModel):
    host: StrictStr
    port: StrictInt
    error: StrictStr
