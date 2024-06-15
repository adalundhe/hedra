from typing import Literal, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ClickCommand(BaseModel):
    x_position: StrictInt | StrictFloat
    y_position: StrictInt | StrictFloat
    button: Optional[Literal['left', 'middle', 'right']]='left'
    click_count: Optional[StrictInt]=1
    delay: Optional[StrictInt | StrictFloat]=None
    timeout: StrictInt | StrictFloat