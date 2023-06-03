from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBool,
    AnyHttpUrl
)


from typing import List, Dict, Optional, Union


class GraphQLActionTag(BaseModel):
    name: StrictStr
    value: StrictStr


class GraphQLActionValidator(BaseModel):
    engine: StrictStr
    name: StrictStr
    url: AnyHttpUrl
    method: StrictStr='GET'
    headers: Dict[StrictStr, StrictStr]={}
    query: StrictStr
    operation_name: StrictStr
    variables: Dict[str, Union[StrictStr, StrictInt, StrictFloat, StrictBool, None]]
    weight: Optional[Union[StrictInt, StrictFloat]]
    order: Optional[StrictInt]
    user: Optional[StrictStr]
    tags: List[GraphQLActionTag]=[]
