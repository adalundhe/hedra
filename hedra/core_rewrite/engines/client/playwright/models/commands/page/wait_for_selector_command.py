from typing import Literal, Optional

from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt, StrictStr


class WaitForSelectorCommand(BaseModel):
    selector: StrictStr
    state: Optional[
        Literal[
            'attached', 
            'detached', 
            'hidden', 
            'visible'
        ]
    ] = 'visible'
    strict: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat