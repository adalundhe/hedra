from typing import Literal, Optional

from pydantic import BaseModel, StrictFloat, StrictInt


class ButtonCommand(BaseModel):
    button: Optional[Literal['left', 'middle', 'right']]='left'
    click_count: Optional[StrictInt]=1
    timeout: StrictInt | StrictFloat