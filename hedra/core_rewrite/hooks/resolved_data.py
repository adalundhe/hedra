from pydantic import BaseModel, StrictBytes, StrictInt, StrictBool
from typing import Optional


class ResolvedData(BaseModel):
    data: StrictBytes
    size: Optional[StrictInt]
    is_stream: Optional[StrictBool]=False