from typing import Optional, Tuple, Optional
from pydantic import BaseModel, Field, StrictStr, StrictBool, StrictInt


class ContextHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    store: Optional[StrictStr]=Field(None, min_length=1)
    load: Optional[StrictStr]=Field(None, min_length=1)
    pre: StrictBool
    order: StrictInt


class ContextValidator:

    def __init__(
        self, 
        *names: Optional[Tuple[str, ...]], 
        store: Optional[str]=None, 
        load: Optional[str]=None, 
        pre: bool=False,
        order: int=1
    ) -> None:
        ContextHookValidator(
            names=names,
            store=store,
            load=load,
            pre=pre,
            order=order
        )
