from typing import Literal, Optional, Tuple

from pydantic import (
    BaseModel,
    StrictBytes,
    StrictInt,
    StrictStr,
)

from hedra.core_rewrite.engines.client.shared.models import URL


class OptimizedGraphQLHTTP2Request(BaseModel):
    call_id: StrictInt
    url: Optional[URL] = None
    method: Literal["GET", "POST"]
    encoded_params: Optional[StrictStr | StrictBytes] = None
    encoded_auth: Optional[StrictStr | StrictBytes] = None
    encoded_cookies: Optional[
        StrictStr
        | StrictBytes
        | Tuple[StrictStr, StrictStr]
        | Tuple[StrictBytes, StrictBytes]
    ] = None
    encoded_headers: Optional[StrictBytes] = None
    ecoded_data: Optional[StrictBytes] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True
