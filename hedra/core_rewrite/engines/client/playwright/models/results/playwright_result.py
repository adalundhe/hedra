from typing import Dict, Generic, Literal, Optional, TypeVar

from pydantic import BaseModel, StrictFloat, StrictStr

from hedra.core_rewrite.engines.client.playwright.models.browser import BrowserMetadata

T = TypeVar('T')

class PlaywrightResult(BaseModel, Generic[T]):
    command: StrictStr
    command_args: BaseModel
    metadata: BrowserMetadata
    result: T
    error: Optional[StrictStr]=None
    timings: Dict[
        Literal[
            'command_start',
            'command_end'
        ],
        StrictFloat
    ]={
        'command_start': 0,
        'command_end': 0
    }
    frame: Optional[StrictStr]=None

    class Config:
        arbitrary_types_allowed=True
