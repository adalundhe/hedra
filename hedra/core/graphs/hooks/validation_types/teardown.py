from typing import Optional, Dict, Union
from pydantic import BaseModel


class TeardownValidator(BaseModel):
    key: Optional[str]
    metadata: Optional[Dict[str, Union[int, str, float]]]

    class Config:
        arbitrary_types_allowed = True

    def __init__(__pydantic_self__, key: str=None, metadata: Optional[Dict[str, Union[int, str, float]]]={}) -> None:
        super().__init__(
            key=key,
            metadata=metadata
        )