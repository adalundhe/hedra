from typing import Optional, Dict, Union
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat


class TeardownHookValidator(BaseModel):
    key: Optional[StrictStr]
    metadata: Dict[StrictStr, Union[StrictInt, StrictStr, StrictFloat]]={}

    class Config:
        arbitrary_types_allowed = True


class TeardownValidator:

    def __init__(__pydantic_self__, key: str=None, metadata: Optional[Dict[str, Union[int, str, float]]]={}) -> None:
        TeardownHookValidator(
            key=key,
            metadata=metadata
        )