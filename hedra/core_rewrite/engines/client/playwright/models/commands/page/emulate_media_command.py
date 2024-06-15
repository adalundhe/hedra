from typing import Literal, Optional

from pydantic import BaseModel, StrictFloat, StrictInt


class EmulateMediaCommand(BaseModel):
    media: Optional[
        Literal['null', 'print', 'screen']
    ] = None
    color_scheme: Optional[
        Literal['dark', 'light', 'no-preference', 'null']
    ] = None
    reduced_motion: Optional[
        Literal['no-preference', 'null', 'reduce']
    ] = None
    forced_colors: Optional[
        Literal['active', 'none', 'null']
    ] = None
    timeout: StrictInt | StrictFloat