from typing import Literal, Optional, Tuple

from pydantic import (
    BaseModel,
    StrictBytes,
    StrictInt,
    StrictStr,
)

from hedra.core_rewrite.engines.client.shared.models import URL


class OptimizedHTTP3Request(BaseModel):
    call_id: StrictInt
    url: Optional[URL] = None
    method: Literal[
        "GET",
        "POST",
        "HEAD",
        "OPTIONS",
        "PUT",
        "PATCH",
        "DELETE",
    ]
    encoded_params: Optional[StrictStr | StrictBytes] = None
    encoded_auth: Optional[StrictStr | StrictBytes] = None
    encoded_cookies: Optional[
        StrictStr
        | StrictBytes
        | Tuple[StrictStr, StrictStr]
        | Tuple[StrictBytes, StrictBytes]
    ] = None
    encoded_headers: Optional[StrictBytes] = None
    encoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True
