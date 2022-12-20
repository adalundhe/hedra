import json
from typing import Dict, Any, Union
from hedra.core.engines import registered_engines
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Action, MercuryGraphQLHTTP2Client
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes
from hedra.logging import HedraLogger


async def assemble_graphql_http2_action(
    hook: Union[ActionHook, TaskHook],
    hook_action: Dict[str, Any],
    persona: DefaultPersona,
    config: Config,
    metadata_string: str
):

    logger = HedraLogger()
    logger.initialize()
    
    action_name = hook_action.get('name')
    hook_type_name = RequestTypes.GRAPHQL_HTTP2.capitalize()

    await logger.filesystem.aio['hedra.core'].debug(
        f'{metadata_string} - Assembling {hook_type_name} Hook - {action_name}'
    )

    action_hooks = hook_action.get('hooks', {})
    action_hook_names = action_hooks.get('names')

    before_hook_name = action_hook_names.get('before')
    after_hook_name = action_hook_names.get('after')
    check_hook_names = action_hook_names.get('checks', [])
    listener_names = action_hook_names.get('listeners', [])
    channel_hook_names = action_hook_names.get('channels', [])

    if before_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {action_name} - Found Before Hook - {before_hook_name}'
        )

    if after_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {action_name} - Found After Hook - {after_hook_name}'
        )

    for check_hook_name in check_hook_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {action_name} - Found Check Hook - {check_hook_name}'
        )

    for channel_hook_name in channel_hook_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {action_name} - Found Channel Hook - {channel_hook_name}'
        )
        
    for listener_name in listener_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {action_name} - Found Listener - {listener_name}'
        )

    hook.session: MercuryGraphQLHTTP2Client = registered_engines.get(RequestTypes.GRAPHQL_HTTP2)(
        concurrency=persona.batch.size,
        timeouts=hook_action.get('timeouts'),
        reset_connections=hook_action.get('reset_connections')
    )

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Connect Timeout: {hook.session.timeouts.connect_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Read Timeout: {hook.session.timeouts.socket_read_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Total Timeout: {hook.session.timeouts.total_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Reset Connections: {hook.session.pool.reset_connections}')

    action_url = hook_action.get('url', {})
    action_headers = hook_action.get('headers', {})
    action_data = hook_action.get('data', {})
    action_metadata = hook_action.get('metadata', {})

    hook.action: GraphQLHTTP2Action = GraphQLHTTP2Action(
        action_name,
        action_url.get('url'),
        hook_action.get('method'),
        headers=action_headers.get('headers'),
        data=action_data.get('data'),
        user=action_metadata.get('user'),
        tags=action_metadata.get('tags')
    )

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - URL: {hook.action.url.full}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Method: {hook.action.method}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Headers: {json.dumps(hook.action.headers)}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Data: {json.dumps(hook.action.data)}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - User: {hook.action.metadata.user}')

    for tag in hook.action.metadata.tags_to_string_list():
        await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Tag: {tag}')

    hook.action.hooks.notify = action_hooks.get('notify', False)
    hook.action.hooks.listen = action_hooks.get('listen', False)

    if before_hook_name:
        before_hook = registrar.all.get(before_hook_name)
        hook.action.hooks.before = before_hook.call

    if after_hook_name:
        after_hook = registrar.all.get(after_hook_name)
        hook.action.hooks.after = after_hook.call

    hook.action.hooks.checks = []
    hook.action.hooks.checks.extend(check_hook_names)

    hook.action.hooks.channels = []
    for channel_hook_name in channel_hook_names:
        channel = registrar.all.get(channel_hook_name)
        hook.action.hooks.channels.append(channel.call) 

    hook.action.hooks.listeners = listener_names
    
    hook.action.url.ip_addr = action_url.get('ip_addr')
    hook.action.url.port = action_url.get('port')
    hook.action.url.socket_config = action_url.get('socket_config')
    hook.action.url.is_ssl = action_url.get('is_ssl')

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - IP Addr: {hook.action.url.ip_addr}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Port: {hook.action.url.port}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Socket Family: {hook.action.url.family}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Socket Protocol: {hook.action.url.protocol}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Is SSL: {hook.action.url.is_ssl}')

    if hook.action.url.is_ssl:
        hook.action.ssl_context = hook.session.ssl_context

    hook.action.encoded_headers = action_headers.get('encoded_headers')
    hook.action.encoded_data = action_data.get('encoded_data')

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Setup complete')

    return hook