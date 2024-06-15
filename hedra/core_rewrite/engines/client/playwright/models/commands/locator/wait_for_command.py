from typing import Literal

from pydantic import BaseModel, StrictFloat, StrictInt


class WaitForCommand(BaseModel):
    state: Literal['attached', 'detached', 'visible', 'hidden']='visible'
    timeout: StrictInt | StrictFloat