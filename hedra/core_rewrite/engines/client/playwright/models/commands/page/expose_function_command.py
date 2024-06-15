from typing import Any, Callable

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class ExposeFunctionCommand(BaseModel):
    name: StrictStr
    callback: Callable[..., Any]
    timeout: StrictInt | StrictFloat
    