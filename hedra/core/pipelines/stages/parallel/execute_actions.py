import asyncio
import traceback
import dill
import time
from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryWebsocketClient,
    MercuryUDPClient
)

from hedra.core.engines.types.http import HTTPAction
from hedra.core.engines.types.http2 import HTTP2Action
from hedra.core.engines.types.graphql import GraphQLAction
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Action
from hedra.core.engines.types.grpc import GRPCAction
from hedra.core.engines.types.playwright import PlaywrightResult
from hedra.core.engines.types.websocket import WebsocketAction
from hedra.core.engines.types.udp import UDPAction
from hedra.core.engines.types.common.types import RequestTypes

from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType

from hedra.core.personas import get_persona
from .types import PartitionMethod


def execute_actions(parallel_config: str):

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        parallel_config = dill.loads(parallel_config)
        partition_method = parallel_config.get('partition_method')
        persona_config = parallel_config.get('config')
        workers = parallel_config.get('workers')
        worker_id = parallel_config.get('worker_id')

        if partition_method == PartitionMethod.BATCHES:

            if workers == worker_id:
                persona_config.batch_size = int(persona_config.batch_size/workers) + (persona_config.batch_size%workers)
            
            else:
                persona_config.batch_size = int(persona_config.batch_size/workers)

        persona = get_persona(persona_config)
        persona.workers = workers

        hooks = {
            HookType.ACTION: []
        }
        for hook_action in parallel_config.get('hooks'):
            
            action_name = hook_action.get('name')

            hook = Hook(
                hook_action.get('hook_name'),
                action_name,
                None,
                hook_action.get('stage'),
                hook_type=HookType.ACTION

            )

            action_hooks = hook_action.get('hooks', {})
            before_hook_name = action_hooks.get('before')
            after_hook_name = action_hooks.get('after')
            check_hook_names = action_hooks.get('checks')

            action_type = hook_action.get('type')

            if action_type == RequestTypes.HTTP:
                
                hook.session = MercuryHTTPClient(
                    concurrency=persona.batch.size,
                    timeouts=hook_action.get('timeouts'),
                    reset_connections=hook_action.get('reset_connections')
                )

                action_url = hook_action.get('url')
                action_headers = hook_action.get('headers')
                action_data = hook_action.get('data')
                action_metadata = hook_action.get('metadata')

                hook.action = HTTPAction(
                    action_name,
                    action_url.get('url'),
                    hook_action.get('method'),
                    headers=action_headers.get('headers'),
                    data=action_data.get('data'),
                    user=action_metadata.get('user'),
                    tags=action_metadata.get('tags')
                )

                if before_hook_name:
                    before_hook = registrar.all.get(before_hook_name)
                    hook.action.hooks.before = before_hook.call

                if after_hook_name:
                    after_hook = registrar.all.get(after_hook_name)
                    hook.action.hooks.after = after_hook.call

                hook.action.hooks.checks = []
                for check_hook_name in check_hook_names:
                    hook.action.hooks.checks.append(check_hook_name)

                hook.action.url.ip_addr = action_url.get('ip_addr')
                hook.action.url.port = action_url.get('port')
                hook.action.url.socket_config = action_url.get('socket_config')
                hook.action.url.is_ssl = action_url.get('is_ssl')

                if hook.action.url.is_ssl:
                    hook.action.ssl_context = hook.session.ssl_context

                hook.action.encoded_headers = action_headers.get('encoded_headers')
                hook.action.encoded_data = action_data.get('encoded_data')

                hooks[HookType.ACTION].append(hook)

            elif action_type == RequestTypes.HTTP2:

                hook.session = MercuryHTTP2Client(
                    concurrency=persona.batch.size,
                    timeouts=hook_action.get('timeouts'),
                    reset_connections=hook_action.get('reset_connections')
                )

                action_url = hook_action.get('url')
                action_headers = hook_action.get('headers')
                action_data = hook_action.get('data')
                action_metadata = hook_action.get('metadata')

                hook.action = HTTP2Action(
                    action_name,
                    action_url.get('url'),
                    hook_action.get('method'),
                    headers=action_headers.get('headers'),
                    data=action_data.get('data'),
                    user=action_metadata.get('user'),
                    tags=action_metadata.get('tags')
                )

                if before_hook_name:
                    before_hook = registrar.all.get(before_hook_name)
                    hook.action.hooks.before = before_hook.call

                if after_hook_name:
                    after_hook = registrar.all.get(after_hook_name)
                    hook.action.hooks.after = after_hook.call

                hook.action.hooks.checks = []
                for check_hook_name in check_hook_names:
                    hook.action.hooks.checks.append(check_hook_name)

                hook.action.url.ip_addr = action_url.get('ip_addr')
                hook.action.url.port = action_url.get('port')
                hook.action.url.socket_config = action_url.get('socket_config')
                hook.action.url.is_ssl = action_url.get('is_ssl')

                if hook.action.url.is_ssl:
                    hook.action.ssl_context = hook.session.ssl_context

                hook.action.encoded_headers = action_headers.get('encoded_headers')
                hook.action.encoded_data = action_data.get('encoded_data')

                hooks[HookType.ACTION].append(hook)

            elif action_type == RequestTypes.UDP:

                hook.session = MercuryUDPClient(
                    concurrency=persona.batch.size,
                    timeouts=hook_action.get('timeouts'),
                    reset_connections=hook_action.get('reset_connections')
                )

                action_url = hook_action.get('url')
                action_data = hook_action.get('data')
                action_metadata = hook_action.get('metadata')

                hook.action = UDPAction(
                    action_name,
                    action_url.get('url'),
                    wait_for_response=hook_action.get('wait_for_response'),
                    data=action_data.get('data'),
                    user=action_metadata.get('user'),
                    tags=action_metadata.get('tags')
                )

                if before_hook_name:
                    before_hook = registrar.all.get(before_hook_name)
                    hook.action.hooks.before = before_hook.call

                if after_hook_name:
                    after_hook = registrar.all.get(after_hook_name)
                    hook.action.hooks.after = after_hook.call

                hook.action.hooks.checks = []
                for check_hook_name in check_hook_names:
                    hook.action.hooks.checks.append(check_hook_name)

                hook.action.url.ip_addr = action_url.get('ip_addr')
                hook.action.url.port = action_url.get('port')
                hook.action.url.socket_config = action_url.get('socket_config')
                hook.action.url.is_ssl = action_url.get('is_ssl')

                if hook.action.url.is_ssl:
                    hook.action.ssl_context = hook.session.ssl_context

                hook.action.encoded_data = action_data.get('encoded_data')

                hooks[HookType.ACTION].append(hook)


        persona.setup(hooks)

        results = loop.run_until_complete(persona.execute())

        return {
            'results': results,
            'total_elapsed': persona.total_elapsed
        }
    except Exception as e:
        raise e