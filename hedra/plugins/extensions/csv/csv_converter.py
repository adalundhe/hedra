import os
import re
import csv
import json
import asyncio
import functools
import uuid
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.timeouts import Timeouts

from hedra.core.engines.types.http import (
    HTTPAction,
    MercuryHTTPClient
)

from hedra.core.engines.types.http2 import (
    HTTP2Action,
    MercuryHTTP2Client
)

from hedra.core.engines.types.http3 import (
    HTTP3Action,
    MercuryHTTP3Client
)

from hedra.core.engines.types.graphql import (
    GraphQLAction,
    MercuryGraphQLClient
)

from hedra.core.engines.types.graphql_http2 import (
    GraphQLHTTP2Action,
    MercuryGraphQLHTTP2Client
)

from hedra.core.engines.types.grpc import (
    GRPCAction,
    MercuryGRPCClient
)

from hedra.core.engines.types.udp import (
    UDPAction,
    MercuryUDPClient
)

from hedra.core.engines.types.websocket import (
    WebsocketAction,
    MercuryWebsocketClient
)

from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.base.stage import Stage
from hedra.plugins.types.extension.types import ExtensionType
from hedra.plugins.types.extension import (
    ExtensionPlugin,
    execute,
    prepare
)
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import (
    Dict, 
    List, 
    Any,
    Callable,
    Union
)
from hedra.plugins.extensions.base.generator_action import GeneratorAction


@unstable
class CSVConverter(ExtensionPlugin):
    extension_type = ExtensionType.GENERATOR

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self._csv: List[Dict[str, Any]] = []
        self._action_data: List[ActionHook] = []
        self.name = CSVConverter.__name__
        self.extension_type = self.extension_type

        self._actions: Dict[
            str, 
            Callable[
                [GeneratorAction], 
                Union[
                    HTTPAction,
                    HTTP2Action,
                    HTTP3Action,
                    GraphQLAction,
                    GraphQLHTTP2Action,
                    GRPCAction,
                    UDPAction,
                    WebsocketAction
                ]
            ]
        ] = {
            'http': lambda csv_action: HTTPAction(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'http2': lambda csv_action: HTTP2Action(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'http3': lambda csv_action: HTTP3Action(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'graphql': lambda csv_action: GraphQLAction(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'graphqlh2': lambda csv_action: GraphQLHTTP2Action(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'grpc': lambda csv_action: GRPCAction(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'udp': lambda csv_action: UDPAction(
                csv_action.name,
                csv_action.url,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            ),
            'websocket': lambda csv_action: WebsocketAction(
                csv_action.name,
                csv_action.url,
                method=csv_action.method,
                headers=csv_action.headers,
                data=csv_action.data,
                user=csv_action.user,
                tags=[
                    tag.dict() for tag in csv_action.tags
                ]
            )
        }

        self._session: Dict[
            str,
            Callable[
                [Config],
                Union[
                    MercuryHTTPClient,
                    MercuryHTTP2Client,
                    MercuryHTTP3Client,
                    GraphQLAction,
                    GraphQLHTTP2Action,
                    GRPCAction,
                    UDPAction,
                    WebsocketAction
                ]
            ]
        ] = {
            'http': lambda config: MercuryHTTPClient(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'http2': lambda config: MercuryHTTP2Client(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'http3': lambda config: MercuryHTTP3Client(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'graphql': lambda config: MercuryGraphQLClient(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'graphqlh2': lambda config: MercuryGraphQLHTTP2Client(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'grpc': lambda config: MercuryGRPCClient(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'udp': lambda config: MercuryUDPClient(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            ),
            'websocket': lambda config: MercuryWebsocketClient(
                concurrency=config.batch_size,
                timeouts=Timeouts(
                    connect_timeout=config.connect_timeout,
                    total_timeout=config.request_timeout
                ),
                reset_connections=config.reset_connections,
                tracing_session=config.tracing
            )
        }
    
    @prepare()
    async def load(
        self,
        persona_config: Config=None,
        execute_stage: Stage=None
    ) -> Dict[str, List[ActionHook]]:

        self._loop = asyncio.get_event_loop()
        await self._load_csv_file(
            persona_config,
            execute_stage
        )

        return await self._to_actions(
            persona_config,
            execute_stage
        )
    
    @execute()
    async def convert(
        self,
        action_data: List[ActionHook]=[],
        execute_stage: Stage=None
    ) -> Dict[str, Stage]:

        action_hooks = execute_stage.hooks[HookType.ACTION]

        if len(execute_stage.hooks[HookType.ACTION]) > 0:
            max_existing_hook_order = max([
                hook.order for hook in action_hooks
            ])

        else:
            max_existing_hook_order = 0
        
        sequence_order = max_existing_hook_order + 1

        for hook in action_data:
            hook.order = sequence_order

            sequence_order += 1

        action_hooks.extend(action_data)

        execute_stage.hooks[HookType.ACTION] = action_hooks

        return {
            'execute_stage': execute_stage
        }

    async def _load_csv_file(
        self,
        config: Config,
        execute_stage: Stage
    ) -> None:

        actions_filepath = await self._loop.run_in_executor(
            None,
            functools.partial(
                os.path.abspath,
                config.actions_filepaths.get(execute_stage.name)
            )
        )

        csv_file = await self._loop.run_in_executor(
            None,
            functools.partial(
                open,
                actions_filepath
            )
        )

        csv_reader = csv.DictReader(csv_file)

        for csv_action in csv_reader:
            self._csv.append(csv_action)

        await self._loop.run_in_executor(
            None,
            csv_file.close
        )

    async def _to_actions(
        self,
        config: Config,
        execute_stage: Stage
    ) -> List[ActionHook]:
        
        action_data: List[ActionHook] = []

        for action_item in self._csv:


            normalized_headers = {}
            action_item_headers = action_item.get('headers', {})
            for header_name, header in action_item_headers.items():
                normalized_headers[header_name] = header

            
            content_type: str = normalized_headers.get('content-type')
            action_item_data = action_item.get('data')
            if content_type.lower() == 'application/json' and action_item_data:
                action_item_data = json.loads(action_item_data)
            
            csv_action = GeneratorAction(
                engine=action_item.get('engine', "http"),
                name=action_item.get("name"),
                url=action_item.get('url'),
                method=action_item.get('method', "GET"),
                params=action_item.get('params', {}),
                query=action_item.get('query'),
                headers=normalized_headers,
                data=action_item_data,
                weight=action_item.get('weight', 1),
                order=action_item.get("order", 1),
                user=action_item.get('user'),
                tags=action_item.get('tags', [])
            )

            action = self._actions.get(
                csv_action.engine,
                self._actions.get('http')
            )(csv_action)

            session = self._session.get(
                csv_action.engine,
                self._session.get('http')
            )(config)

            await action.url.lookup()
            action.setup()

            hook = ActionHook(
                f'{execute_stage.name}.{csv_action.name}',
                csv_action.name,
                None
            )

            hook.session = session
            hook.action = action
            hook.stage = execute_stage.name
            hook.stage_instance = execute_stage
            hook.context = SimpleContext()
            hook.hook_id = uuid.uuid4()

            action_data.append(hook)

        return {
            'action_data': action_data
        }
    