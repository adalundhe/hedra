from typing import Literal, Optional

from pydantic import BaseModel, StrictBytes, StrictInt

from hedra.core_rewrite.engines.client.shared.models import URL


class OptimizedGRPCRequest(BaseModel):
    call_id: StrictInt
    url: Optional[URL] = None
    method: Literal["GET", "POST"]
    encoded_headers: Optional[StrictBytes] = None
    encoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True
