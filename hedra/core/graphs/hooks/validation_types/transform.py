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
    load: Optional[StrictStr]=None
    store: Optional[StrictStr]=None
    pre: Optional[StrictBool]
    order: Optional[StrictInt]

    @validator('names')
    def validate_names(cls, vals):
        assert len(vals) == len(set(vals))


class TransformValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        load: Optional[str]=None, 
        store: Optional[str]=None,
        pre: bool=False,
        order: int=1
    ) -> None:
        TransformHookValidator(
            names=names,
            load=load,
            store=store,
            pre=pre,
            order=order
        )
