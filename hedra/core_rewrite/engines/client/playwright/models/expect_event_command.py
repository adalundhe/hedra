from typing import (
    Callable,
    Literal,
    Optional,
)

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class ExpectEventCommand(BaseModel):
    event: Literal[
        'close',
        'console',
        'crash',
        'dialog',
        'domcontentloaded',
        'download',
        'filechooser',
        'frameattached',
        'framedetached',
        'framenavigated',
        'load',
        'pageerror',
        'popup',
        'request',
        'requestfailed',
        'requestfinished',
        'response',
        'websocket',
        'worker'
    ]
    predicate: Optional[
        Callable[
            [StrictStr],
            bool
        ]
    ]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True