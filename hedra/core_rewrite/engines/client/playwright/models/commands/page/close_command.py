from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class CloseCommand(BaseModel):
    run_before_unload: Optional[StrictBool]=None
    reason: Optional[StrictStr]
    timeout: StrictInt | StrictFloat