from typing import Optional, Tuple, Optional
from pydantic import BaseModel, Field, StrictStr, StrictBool, StrictInt


class ContextHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    order: StrictInt


class ContextValidator:

    def __init__(
        self, 
        *names: Optional[Tuple[str, ...]], 
        order: int=1
    ) -> None:
        ContextHookValidator(
            names=names,
            order=order
        )
