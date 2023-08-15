from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBool,
    AnyHttpUrl
)
from typing import List, Dict, Optional, Union, Any


class PlaywrightOptionsValidator(BaseModel):
    event: Optional[StrictStr]
    option: Any
    is_checked: Optional[StrictBool]
    timeout: Optional[Union[StrictInt, StrictFloat]]
    extra: Optional[Dict[str, Any]]
    switch_by: Optional[StrictStr]


class PlaywrightInputValidator(BaseModel):
    key: Optional[StrictStr]
    text: Optional[StrictStr]
    expression: Optional[StrictStr]
    args: Optional[List[Any]]
    filepath: Optional[StrictStr]
    file: Optional[StrictStr]
    path: Optional[StrictStr]
    option: Optional[Union[StrictStr, StrictBool, StrictInt, StrictFloat]]
    by_label: StrictBool=False
    by_value: StrictBool=False


class PlaywrightURLValidator(BaseModel):
    location: Optional[AnyHttpUrl]
    headers: Dict[str, str]


class PlaywrightPageValidator(BaseModel):
    selector: Optional[StrictStr]
    attribute: Optional[StrictStr]
    x_coordinate: Optional[Union[StrictInt, StrictFloat]]
    y_coordinate: Optional[Union[StrictInt, StrictFloat]]
    frame: Optional[StrictInt]


class PlaywrightActionTag(BaseModel):
    name: StrictStr
    value: StrictStr


class PlaywrightActionValidator(BaseModel):
    engine: StrictStr
    name: StrictStr
    command: StrictStr
    page: Optional[PlaywrightPageValidator]
    url: Optional[PlaywrightURLValidator]
    input: Optional[PlaywrightInputValidator]
    options: Optional[PlaywrightOptionsValidator]
    weight: Optional[Union[StrictInt, StrictFloat]]
    order: Optional[StrictInt]
    user: Optional[StrictStr]
    tags: List[PlaywrightActionTag]=[]

