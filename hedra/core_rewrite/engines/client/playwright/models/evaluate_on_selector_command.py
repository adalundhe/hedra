from typing import Any, Optional

from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class EvaluateOnSelectorCommand(BaseModel):
    selector: StrictStr
    expression: StrictStr
    arg: Optional[Any]=None
    strict: Optional[StrictBool]=None
    timeout: Optional[StrictInt | StrictFloat]=None