import json
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.engines.types.playwright.command import PlaywrightCommand
from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.websocket.action import WebsocketAction
from typing import List, Union, Callable, Dict, Any
from .serializer_types import (
    GraphQLSerializer,
    GraphQLHTTP2Serializer,
    GRPCSerializer,
    HTTPSerializer,
    HTTP2Serializer,
    HTTP3Serializer,
    PlaywrightSerializer,
    UDPSerializer,
    WebsocketSerializer
)


class Serializer:

    def __init__(self) -> None:
        self._serializer_types: Dict[
            str,
            Callable[
                ...,
                Union[
                    GraphQLSerializer,
                    GraphQLHTTP2Serializer,
                    GRPCSerializer,
                    HTTPSerializer,
                    HTTP2Serializer,
                    HTTP3Serializer,
                    PlaywrightSerializer,
                    UDPSerializer,
                    WebsocketSerializer
                ]
            ]
        ] = {
            RequestTypes.GRAPHQL: lambda: GraphQLSerializer(),
            RequestTypes.GRAPHQL_HTTP2: lambda: GraphQLHTTP2Serializer(),
            RequestTypes.GRPC: lambda: GRPCSerializer(),
            RequestTypes.HTTP: lambda: HTTPSerializer(),
            RequestTypes.HTTP2: lambda: HTTP2Serializer(),
            RequestTypes.HTTP3: lambda: HTTP3Serializer(),
            RequestTypes.PLAYWRIGHT: lambda: PlaywrightSerializer(),
            RequestTypes.UDP: lambda: UDPSerializer(),
            RequestTypes.WEBSOCKET: lambda: WebsocketSerializer()
        }

        self._active_serializers: Dict[
            str,
            Union[
                GraphQLSerializer,
                GraphQLHTTP2Serializer,
                GRPCSerializer,
                HTTPSerializer,
                HTTP2Serializer,
                HTTP3Serializer,
                PlaywrightSerializer,
                UDPSerializer,
                WebsocketSerializer
            ] 
        ] = {}

    def serialize_action(
        self,
        action: Union[
            GraphQLAction,
            GraphQLHTTP2Action,
            GRPCAction,
            HTTPAction,
            HTTP2Action,
            HTTP3Action,
            PlaywrightCommand,
            UDPAction,
            WebsocketAction
        ]
    ):
        serializer = self._active_serializers.get(action.type)

        if serializer is None and action.type in self._serializer_types:
            serializer = self._serializer_types.get(action.type)()
            self._active_serializers[action.type] = serializer

        serializable_action = serializer.serialize_action(action)
        return json.dumps(serializable_action)
    
    def deserialize_action(
        self,
        serialized_action: Union[str, bytes]
    ):
        deserialized_action: Dict[str, Any] = json.loads(serialized_action)
        action_type = deserialized_action.get('type', RequestTypes.HTTP)

        serializer = self._active_serializers.get(action_type)
    
        if serializer is None and action_type in self._serializer_types:
            serializer = self._serializer_types.get(action_type)()
            self._active_serializers[action_type] = serializer

        return serializer.deserialize_action(deserialized_action)