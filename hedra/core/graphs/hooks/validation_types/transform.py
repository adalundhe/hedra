from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    validator, 
    StrictStr, 
    StrictInt, 
    StrictBool
)


class TransformHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    pre: Optional[StrictBool]
    order: Optional[StrictInt]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class TransformValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        pre: bool=False,
        order: int=1
    ) -> None:
        TransformHookValidator(
            names=names,
            pre=pre,
            order=order
        )
