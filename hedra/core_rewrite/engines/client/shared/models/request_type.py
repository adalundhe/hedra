from enum import Enum


class RequestType(Enum):
    HTTP = "HTTP"
    HTTP2 = "HTTP2"
    HTTP3 = "HTTP3"
    WEBSOCKET = "WEBSOCKET"
    GRAPHQL = "GRAPHQL"
    GRAPHQL_HTTP2 = "GRAPHQL_HTTP2"
    GRPC = "GRPC"
    PLAYWRIGHT = "PLAYWRIGHT"
    UDP = "UDP"
    TASK = "TASK"
    CUSTOM = "CUSTOM"
