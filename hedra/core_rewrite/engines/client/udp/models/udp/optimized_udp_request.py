from typing import Any, Optional

from pydantic import BaseModel, StrictBytes, StrictInt


class OptimizedUDPRequest(BaseModel):
    call_id: StrictInt
    socket_info: Optional[Any] = None
    encoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3
