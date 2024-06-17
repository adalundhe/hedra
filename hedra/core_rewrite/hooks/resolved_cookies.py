from typing import Tuple

from pydantic import BaseModel, StrictBytes, StrictStr


class ResolvedCookies(BaseModel):
    cookies: (
        StrictBytes
        | Tuple[StrictBytes, StrictBytes]
        | StrictStr
        | Tuple[StrictStr, StrictStr]
    )
