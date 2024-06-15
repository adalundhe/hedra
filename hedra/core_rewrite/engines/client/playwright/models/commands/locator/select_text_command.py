from typing import Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
)


class SelectTextCommand(BaseModel):
    force: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat