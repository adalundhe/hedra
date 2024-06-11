from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, StrictFloat, StrictStr

from hedra.core_rewrite.engines.client.playwright.models.browser import BrowserMetadata


class PlaywrightResult(BaseModel):
    command: StrictStr
    command_args: BaseModel
    metadata: BrowserMetadata
    result: Any
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

    class Config:
        arbitrary_types_allowed=True
