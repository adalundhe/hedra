import dill
import traceback
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.engines.types.playwright.command import PlaywrightCommand
from hedra.core.engines.types.task.task import Task
from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.websocket.action import WebsocketAction
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from typing import Union, Callable, Dict, Any
from .serializer_types import (
    GraphQLSerializer,
    GraphQLHTTP2Serializer,
    GRPCSerializer,
    HTTPSerializer,
    HTTP2Serializer,
    HTTP3Serializer,
    PlaywrightSerializer,
    TaskSerializer,
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
                    TaskSerializer,
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
            RequestTypes.TASK: lambda: TaskSerializer(),
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
                TaskSerializer,
                UDPSerializer,
                WebsocketSerializer
            ] 
        ] = {}

    def serialize(
        self,
        hook: Union[ActionHook, TaskHook]
    ):
        action: Action = hook.action
        serializer = self._active_serializers.get(action.type)

        if serializer is None and action.type in self._action_serializers:
            serializer = self._action_serializers.get(action.type)()
            self._active_serializers[action.type] = serializer

        serializable = serializer.to_serializable(action)
        serializable_hook = hook.to_dict()
        serializable_client_config = hook.session.config_to_dict()

        return dill.dumps({
            'hook': serializable_hook,
            'action': serializable,
            'client_config': serializable_client_config
        })
    
    def deserialize(
        self,
        serialized_hook: Union[str, bytes]
    ):
        deserialized_hook: Dict[str, Any] = dill.loads(serialized_hook)

        deserialized_hook = deserialized_hook.get('hook', {})
        deserialized_action: Dict[str, Any] = deserialized_hook.get('action', {})
        deserialized_client_config = deserialized_hook.get('client_config', {})

        action_type = deserialized_action.get('type', RequestTypes.HTTP)

        serializer = self._active_serializers.get(action_type)
    
        if serializer is None and action_type in self._action_serializers:
            serializer = self._action_serializers.get(action_type)()
            self._active_serializers[action_type] = serializer

        if action_type == RequestTypes.TASK:

            action_hook = TaskHook(
                deserialized_hook.get('name'),
                deserialized_hook.get('shortname'),
                None,
                *deserialized_hook.get('names', []),
                weight=deserialized_hook.get('weight'),
                order=deserialized_hook.get('order'),
                skip=deserialized_hook.get('skip'),
                metadata={
                    'user': deserialized_hook.get('user'),
                    'tags': deserialized_hook.get('tags')
                }
            )

            action = serializer.deserialize_task(deserialized_action)
        
        else:

            action_hook = ActionHook(
                deserialized_hook.get('name'),
                deserialized_hook.get('shortname'),
                None,
                *deserialized_hook.get('names', []),
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
        