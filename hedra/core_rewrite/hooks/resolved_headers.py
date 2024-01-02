from pydantic import BaseModel, StrictBytes
from typing import List


class ResolvedHeaders(BaseModel):
    headers: StrictBytes | List[StrictBytes]