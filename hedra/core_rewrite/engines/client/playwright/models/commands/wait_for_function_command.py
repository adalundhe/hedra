from typing import Any, Literal, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class WaitForFunctionCommand(BaseModel):
    expression: StrictStr
    arg: Optional[Any]=None
    polling: Optional[StrictFloat | Literal['raf']]=None
    timeout: StrictInt | StrictFloat