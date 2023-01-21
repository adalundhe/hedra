from typing import Optional
from pydantic import BaseModel, Field


class ContextValidator(BaseModel):
    store_key: str=Field(..., min_length=1)
    load_key: Optional[str]=Field(None, min_length=1)

    def __init__(__pydantic_self__, store_key: str, load_key: Optional[str]=None) -> None:
        super().__init__(
            store_key=store_key,
            load_key=load_key
        )