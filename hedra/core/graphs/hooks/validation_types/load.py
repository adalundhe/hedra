from typing import Tuple, Optional
from pydantic import BaseModel, Field, StrictStr, StrictInt


class LoadHookValidator(BaseModel):
    names: Optional[Tuple[StrictStr, ...]]
    load_path: StrictStr=Field(..., min_length=1)
    order: StrictInt


class LoadValidator:

    def __init__(self, *names: Tuple[str, ...], load_path: str=None, order: int=1) -> None:
        LoadHookValidator(
            names=names,
            load_path=load_path,
            order=order
        )