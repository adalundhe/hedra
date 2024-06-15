from typing import Literal, Optional

from pydantic import BaseModel, StrictFloat, StrictInt


class ReloadCommand(BaseModel):
    timeout: StrictInt | StrictFloat
    wait_util: Optional[Literal[
        'commit', 
        'domcontentloaded', 
        'load', 
        'networkidle',
    ]]=None