from typing import Literal, Optional

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class GoToCommand(BaseModel):
    url: StrictStr
    timeout: StrictInt | StrictFloat
    wait_util: Optional[
        Literal[
            "commit",
            "domcontentloaded",
            "load",
            "networkidle",
        ]
    ] = None
    referrer: Optional[StrictStr] = None
