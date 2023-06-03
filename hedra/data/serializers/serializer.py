import dill
import traceback
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql.result import GraphQLResult
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.graphql_http2.result import GraphQLHTTP2Result
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.grpc.result import GRPCResult
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http.result import HTTPResult
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http2.result import HTTP2Result
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.engines.types.http3.result import HTTP3Result
from hedra.core.engines.types.playwright.command import PlaywrightCommand
from hedra.core.engines.types.playwright.result import PlaywrightResult
from hedra.core.engines.types.task.task import Task
from hedra.core.engines.types.task.result import TaskResult
from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.udp.result import UDPResult
from hedra.core.engines.types.websocket.action import WebsocketAction
from hedra.core.engines.types.websocket.result import WebsocketResult
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
    Task,
    UDPAction,
    WebsocketAction
]


Result = Union[
    GraphQLResult,
    GraphQLHTTP2Result,
    GRPCResult,
    HTTPResult,
    HTTP2Result,
    HTTP3Result,
    PlaywrightResult,
    TaskResult,
    UDPResult,
    WebsocketResult
]


class Serializer:

    def __init__(self) -> None:
        self._serializers: Dict[
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

    def serialize_action(
        self,
        hook: Union[ActionHook, TaskHook]
    ):
        action: Action = hook.action
        serializer = self._active_serializers.get(action.type)

        if serializer is None and action.type in self._serializers:
            serializer = self._serializers.get(action.type)()
            self._active_serializers[action.type] = serializer

        serializable_action = serializer.action_to_serializable(action)
        serializable_hook = hook.to_dict()
        serializable_client_config = hook.session.config_to_dict()

        return dill.dumps({
            'hook': serializable_hook,
            'action': serializable_action,
            'client_config': serializable_client_config
        })
    
    def deserialize_action(
        self,
        serialized_hook: Union[str, bytes]
    ):
        deserialized_hook: Dict[str, Any] = dill.loads(serialized_hook)

        deserialized_hook_config = deserialized_hook.get('hook', {})
        deserialized_action: Dict[str, Any] = deserialized_hook.get('action', {})
        deserialized_client_config = deserialized_hook.get('client_config', {})

        action_type = deserialized_action.get('type', RequestTypes.HTTP)

        serializer = self._active_serializers.get(action_type)
    
        if serializer is None and action_type in self._serializers:
            serializer = self._serializers.get(action_type)()
            self._active_serializers[action_type] = serializer

        if action_type == RequestTypes.TASK:

            action_hook = TaskHook(
                deserialized_hook_config.get('name'),
                deserialized_hook_config.get('shortname'),
                None,
                *deserialized_hook_config.get('names', []),
                weight=deserialized_hook_config.get('weight'),
                order=deserialized_hook_config.get('order'),
                skip=deserialized_hook_config.get('skip'),
                metadata={
                    'user': deserialized_hook_config.get('user'),
                    'tags': deserialized_hook_config.get('tags')
                }
            )

            action = serializer.deserialize_task(deserialized_action)
        
        else:

            action_hook = ActionHook(
                deserialized_hook_config.get('name'),
                deserialized_hook_config.get('shortname'),
                None,
                *deserialized_hook_config.get('names', []),
                weight=deserialized_hook_config.get('weight'),
                order=deserialized_hook_config.get('order'),
                skip=deserialized_hook_config.get('skip'),
                metadata={
                    'user': deserialized_hook_config.get('user'),
                    'tags': deserialized_hook_config.get('tags')
                }
            )

            action = serializer.deserialize_action(deserialized_action)

        action_hook.action = action

        session = serializer.deserialize_client_config(deserialized_client_config)
        action_hook.session = session

        return action_hook
    
    def serialize_result(
        self,
        result: Result
    ):
        serializer = self._active_serializers.get(result.type)

        if serializer is None and result.type in self._serializers:
            serializer = self._serializers.get(result.type)()
            self._active_serializers[result.type] = serializer

        serializable = serializer.result_to_serializable(result)

        return dill.dumps(serializable)
    
    def deserialize_result(
        self,
        serialized_result: Union[str, bytes]
    ) -> Result:
        deserialized_result: Dict[str, Any] = dill.loads(serialized_result)

        result_type = deserialized_result.get('type', RequestTypes.HTTP)

        serializer = self._active_serializers.get(result_type)
    
        if serializer is None and result_type in self._serializers:
            serializer = self._serializers.get(result_type)()
            self._active_serializers[result_type] = serializer

        return serializer.deserialize_result(deserialized_result)