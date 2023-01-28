from typing import Tuple, Optional
from pydantic import BaseModel, validator, StrictStr, StrictBool, StrictInt


class EventHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    pre: StrictBool=False
    key: Optional[StrictStr]
    order: StrictInt

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class EventValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        pre: bool=False, 
        key: Optional[str]=None,
        order: int=1
    ) -> None:
        EventHookValidator(
            names=names,
            pre=pre,
            key=key,
            order=order
        )
