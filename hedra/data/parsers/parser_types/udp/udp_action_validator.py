from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBool,
    AnyHttpUrl,
    Json
)


from typing import List, Optional, Union


class UDPActionTag(BaseModel):
    name: StrictStr
    value: StrictStr


class UDPActionValidator(BaseModel):
    engine: StrictStr
    name: StrictStr
    url: AnyHttpUrl
    wait_for_response: StrictBool=False
    data: Optional[Union[StrictStr, Json]]
    weight: Optional[Union[StrictInt, StrictFloat]]
    order: Optional[StrictInt]
    user: Optional[StrictStr]
    tags: List[UDPActionTag]=[]