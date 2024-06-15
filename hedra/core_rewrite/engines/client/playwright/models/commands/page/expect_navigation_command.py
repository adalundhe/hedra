from typing import Literal, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class ExpectNavigationCommand(BaseModel):
    url: StrictStr
    wait_until: Optional[
        Literal['commit', 'domcontentloaded', 'load', 'networkidle']
    ]=None
    timeout: StrictInt | StrictFloat