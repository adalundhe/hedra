from typing import Any, Optional

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class EvaluateCommand(BaseModel):
    expression: StrictStr
    arg: Optional[Any]=None
    timeout: StrictInt | StrictFloat