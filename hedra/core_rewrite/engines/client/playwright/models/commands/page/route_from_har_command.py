from pathlib import Path
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


class RouteFromHarCommand(BaseModel):
    har: Path | StrictStr
    url: Optional[
        StrictStr | 
        Pattern[str] | 
        Callable[[StrictStr], StrictBool]
    ]=None
    not_found: Literal['abort', 'fallback'] = 'abort'
    update: Optional[StrictBool] = None
    update_content: Optional[
        Literal['attach', 'embed']
    ] = None
    update_mode: Optional[
        Literal['full', 'minimal']
    ] = None
    timeout: StrictInt | StrictFloat