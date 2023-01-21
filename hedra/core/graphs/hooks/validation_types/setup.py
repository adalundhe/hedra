from typing import Optional, Dict, Union
from pydantic import BaseModel, StrictStr, StrictInt, StrictFloat


class SetupHookValidator(BaseModel):
    key: Optional[StrictStr]
    metadata: Dict[StrictStr, Union[StrictInt, StrictStr, StrictFloat]]={}

    class Config:
        arbitrary_types_allowed = True


class SetupValidator:

    def __init__(__pydantic_self__, key: str=None, metadata: Optional[Dict[str, Union[int, str, float]]]={}) -> None:
        SetupHookValidator(
            key=key,
            metadata=metadata
        )