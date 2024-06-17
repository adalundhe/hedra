from typing import Any, Literal, Optional

from pydantic import BaseModel, StrictBytes, StrictInt

NEW_LINE = "\r\n"


class OptimizedHTTPRequest(BaseModel):
    call_id: StrictInt
    socket_info: Optional[Any] = None
    method: Literal[
        "GET",
        "POST",
        "HEAD",
        "OPTIONS",
        "PUT",
        "PATCH",
        "DELETE",
    ]
    encoded_headers: Optional[StrictBytes] = None
    ecoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3
