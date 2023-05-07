from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    AnyHttpUrl,
    Json,
    validator
)

from typing import (
    Union, 
    Optional,
    Dict,
    List
)


class GeneratorActionTag(BaseModel):
    name: StrictStr
    value: StrictStr


class GeneratorAction(BaseModel):
    engine: StrictStr="http"
    name: StrictStr
    url: AnyHttpUrl
    method: StrictStr='GET'
    headers: Dict[StrictStr, StrictStr]={}
    params: Optional[Dict[StrictStr, Union[StrictInt, StrictStr, StrictFloat]]]
    data: Optional[Union[StrictStr, Json]]
    weight: Optional[Union[StrictInt, StrictFloat]]
    order: Optional[StrictInt]
    user: Optional[StrictStr]
    tags: List[GeneratorActionTag]=[]


    @validator("engine")
    def validate_engine(cls, val):
        assert val in [
            "http", 
            "http2", 
            "http3",
            "graphql",
            "graphqlh2",
            "grpc",
            "udp",
            "websocket"
        ], "Invalid engine - please select a supported engine type."

        return val
    

    @validator("data")
    def valdate_graphql_action(cls, val):

        if cls.engine == "graphql" or cls.engine == "graphqlh2":
            assert isinstance(val, dict), "Data must be a dictionary and is required for GraphQL/GraphQLH2 actions."
            assert val.get("query") is not None, "Field query is required for GraphQL/GraphQLH2 actions."
            assert val.get("operation_name") is not None, "Field operation_name is required for GraphQL/GraphQLH2 actions."

        return val