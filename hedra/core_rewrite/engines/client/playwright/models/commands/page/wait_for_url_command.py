from typing import (
    Callable,
    Literal,
    Optional,
    Pattern,
)

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class WaitForUrlCommand(BaseModel):
    url: StrictStr | Pattern[StrictStr] | Callable[[StrictStr], StrictBool]
    wait_until: Optional[
        Literal[
            'commit', 
            'domcontentloaded', 
            'load', 
            'networkidle'
        ]
    ] = None
    timeout: StrictInt | StrictFloat