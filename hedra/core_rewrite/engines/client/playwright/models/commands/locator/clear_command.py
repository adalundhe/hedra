from typing import Optional

from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt


class ClearCommand(BaseModel):
    force: Optional[StrictBool]=None
    no_wait_after: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat
    