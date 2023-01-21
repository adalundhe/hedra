from typing import Tuple, Optional
from pydantic import BaseModel, validator, StrictStr, StrictBool


class EventHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    pre: StrictBool=False
    key: Optional[StrictStr]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class EventValidator:

    def __init__(__pydantic_self__, *names: Tuple[str, ...], pre: bool=False, key: Optional[str]=None) -> None:
        EventHookValidator(
            names=names,
            pre=pre,
            key=key
        )
