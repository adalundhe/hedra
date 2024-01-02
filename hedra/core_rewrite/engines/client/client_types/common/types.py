import socket
from enum import Enum
from typing import Dict, Literal, Tuple, Union


class SocketTypes(Enum):
    DEFAULT=socket.AF_INET
    HTTP2=socket.AF_INET
    UDP=socket.AF_INET
    HTTP3=socket.AF_INET6
    NONE=None

class SocketProtocols(Enum):
    DEFAULT=socket.SOCK_STREAM
    HTTP2=socket.SOCK_STREAM
    HTTP3=socket.SOCK_DGRAM
    UDP=socket.SOCK_DGRAM
    NONE=None


class RequestType(Enum):
    HTTP='HTTP'
    HTTP2='HTTP2'
    HTTP3='HTTP3'
    WEBSOCKET='WEBSOCKET'
    GRAPHQL='GRAPHQL'
    GRAPHQL_HTTP2="GRAPHQL_HTTP2"
    GRPC='GRPC'
    PLAYWRIGHT='PLAYWRIGHT'
    UDP='UDP'
    TASK='TASK'
    CUSTOM='CUSTOM'


class RequestTypesMap:

    def __init__(self) -> None:
        self._types: Dict[
            Literal[
                'graphql',
                'graphqlh2',
                'grpc',
                'http',
                'http2',
                'http3',
                'playwright',
                'udp',
                'websocket'
            ],
            RequestType
        ] = {
            'graphql': RequestType.GRAPHQL,
            'graphqlh2': RequestType.GRAPHQL_HTTP2,
            'grpc': RequestType.GRPC,
            'http': RequestType.HTTP,
            'http2': RequestType.HTTP2,
            'http3': RequestType.HTTP3,
            'playwright': RequestType.PLAYWRIGHT,
            'udp': RequestType.UDP,
            'websocket': RequestType.WEBSOCKET
        }

    def __getitem__(self, request_type: str):
        return self._types.get(request_type)
    
    @classmethod
    def is_http(cls, request_type: RequestType):
        return request_type in [
            RequestType.GRAPHQL,
            RequestType.HTTP,
            RequestType.HTTP3,
            RequestType.WEBSOCKET
        ]
    
    @classmethod
    def is_http2(cls, request_type: RequestType):
        return request_type in [
            RequestType.GRAPHQL_HTTP2,
            RequestType.HTTP2,
            RequestType.GRPC
        ]
    
    @classmethod
    def is_playwright(cls, request_type: RequestType):
        return request_type in [
            RequestType.PLAYWRIGHT
        ]
    
    @classmethod
    def is_udp(cls, request_type: RequestType):
        return request_type in [
            RequestType.UDP
        ]


class ProtocolMap:

    def __init__(self) -> None:
        self.address_families = {
            RequestType.HTTP: SocketTypes.DEFAULT,
            RequestType.HTTP2: SocketTypes.HTTP2,
            RequestType.HTTP3: SocketTypes.HTTP3,
            RequestType.WEBSOCKET: SocketTypes.DEFAULT,
            RequestType.GRAPHQL: SocketTypes.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketTypes.HTTP2,
            RequestType.GRPC: SocketTypes.HTTP2,
            RequestType.UDP: SocketTypes.UDP,
            RequestType.PLAYWRIGHT: SocketTypes.NONE
        }

        self.protocols = {
            RequestType.HTTP: SocketProtocols.DEFAULT,
            RequestType.HTTP2: SocketProtocols.HTTP2,
            RequestType.HTTP3: SocketProtocols.HTTP3,
            RequestType.WEBSOCKET: SocketProtocols.DEFAULT,
            RequestType.GRAPHQL: SocketProtocols.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketTypes.HTTP2,
            RequestType.GRPC: SocketProtocols.HTTP2,
            RequestType.UDP: SocketProtocols.UDP,
            RequestType.PLAYWRIGHT: SocketProtocols.NONE
        }

    def __getitem__(self, key: RequestType) -> Tuple[
        Union[
            SocketTypes,
            None
        ], 
        Union[
            SocketTypes,
            None
        ]
    ]:
        return (
            self.address_families.get(key), 
            self.protocols.get(key)
        )
    
