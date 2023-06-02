import json
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql.client import MercuryGraphQLClient
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.graphql_http2.client import MercuryGraphQLHTTP2Client
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.grpc.client import MercuryGRPCClient
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.engines.types.http3.client import MercuryHTTP3Client
from hedra.core.engines.types.playwright.command import PlaywrightCommand
from hedra.core.engines.types.playwright.client import MercuryPlaywrightClient
from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.udp.client import MercuryUDPClient
from hedra.core.engines.types.websocket.action import WebsocketAction
from hedra.core.engines.types.websocket.client import MercuryWebsocketClient
from hedra.core.hooks.types.action.hook import ActionHook
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


Action = Union[
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


class Serializer:

    def __init__(self) -> None:
        self._action_serializers: Dict[
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
        action_hook: ActionHook
    ):
        action: Action = action_hook.action
        serializer = self._active_serializers.get(action.type)

        if serializer is None and action.type in self._action_serializers:
            serializer = self._action_serializers.get(action.type)()
            self._active_serializers[action.type] = serializer

        serializable_action = serializer.action_to_serializable(action)
        serializable_hook = action_hook.to_dict()
        serializable_client_config = action_hook.session.config_to_dict()

        return json.dumps({
            'hook': serializable_hook,
            'action': serializable_action,
            'client_config': serializable_client_config
        })
    
    def deserialize_action(
        self,
        serialized_action_hook: Union[str, bytes]
    ):
        deserialized_action_hook: Dict[str, Any] = json.loads(serialized_action_hook)

        deserialized_hook = deserialized_action_hook.get('hook', {})
        deserialized_action: Dict[str, Any] = deserialized_action_hook.get('action')
        deserialized_client_config = deserialized_action_hook.get('client_config', {})

        action_type = deserialized_action.get('type', RequestTypes.HTTP)

        serializer = self._active_serializers.get(action_type)
    
        if serializer is None and action_type in self._action_serializers:
            serializer = self._action_serializers.get(action_type)()
            self._active_serializers[action_type] = serializer

        action_hook = ActionHook(
            name=deserialized_hook.get('name'),
            shortname=deserialized_hook.get('shortname'),
            names=deserialized_hook.get('names'),
            weight=deserialized_hook.get('weight'),
            order=deserialized_hook.get('order'),
            skip=deserialized_hook.get('skip'),
            metadata={
                'user': deserialized_hook.get('user'),
                'tags': deserialized_hook.get('tags')
            }
        )

        action = serializer.deserialize_action(deserialized_action)
        action_hook.action = action

        session = serializer.deserialize_client_config(deserialized_client_config)
        action_hook.session = session

        return action_hook