from typing import List, Tuple

from pydantic import BaseModel, StrictBytes


class ResolvedHeaders(BaseModel):
    headers: StrictBytes | List[StrictBytes] | List[Tuple[StrictBytes, StrictBytes]]
