from typing import Optional
from pydantic import BaseModel, Field, StrictStr


class ContextHookValidator(BaseModel):
    store_key: StrictStr=Field(..., min_length=1)
    load_key: Optional[StrictStr]=Field(None, min_length=1)


class ContextValidator:

    def __init__(self, store_key: str, load_key: Optional[str]=None) -> None:
        ContextHookValidator(
            store_key=store_key,
            load_key=load_key
        )
