from typing import Tuple, Optional
from pydantic import (
    BaseModel, 
    Field, 
    StrictStr, 
    StrictInt
)


class SaveHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    save_path: StrictStr=Field(..., min_length=1)
    order: StrictInt


class SaveValidator:

    def __init__(
        self, 
        *names: Tuple[str, ...], 
        save_path: str=None,
        order: int = 1
    ) -> None:
        SaveHookValidator(
            names=names,
            save_path=save_path,
            order=order
        )