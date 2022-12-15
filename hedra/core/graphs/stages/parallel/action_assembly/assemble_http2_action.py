import json
from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http2 import HTTP2Action, MercuryHTTP2Client
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes
from hedra.logging import HedraLogger


async def assemble_http2_action(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona,
    config: Config,
    metadata_string: str
):

    logger = HedraLogger()
    logger.initialize()

    action_name = hook_action.get('name')
    hook_type_name = RequestTypes.HTTP2.capitalize()

    await logger.filesystem.aio['hedra.core'].debug(
        f'{metadata_string} - Assembling {hook_type_name} Hook - {action_name}'
    )

    action_hooks = hook_action.get('hooks', {})

    before_hook_name = action_hooks.get('before')
    after_hook_name = action_hooks.get('after')
    check_hook_names = action_hooks.get('checks')

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

    hook.session: MercuryHTTP2Client = registered_engines.get(RequestTypes.HTTP2)(
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

    hook.action: HTTP2Action = HTTP2Action(
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

    if before_hook_name:
        before_hook = registrar.all.get(before_hook_name)
        hook.action.hooks.before = before_hook.call

    if after_hook_name:
        after_hook = registrar.all.get(after_hook_name)
        hook.action.hooks.after = after_hook.call

    hook.action.hooks.checks = []
    for check_hook_name in check_hook_names:
        hook.action.hooks.checks.append(check_hook_name)
    
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - IP Addr: {hook.action.url.ip_addr}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Port: {hook.action.url.port}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Socket Family: {hook.action.url.family}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Socket Protocol: {hook.action.url.protocol}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Is SSL: {hook.action.url.is_ssl}')

    hook.action.url.ip_addr = action_url.get('ip_addr')
    hook.action.url.port = action_url.get('port')
    hook.action.url.socket_config = action_url.get('socket_config')
    hook.action.url.is_ssl = action_url.get('is_ssl')

    if hook.action.url.is_ssl:
        hook.action.ssl_context = hook.session.ssl_context

    hook.action.encoded_headers = action_headers.get('encoded_headers')
    hook.action.encoded_data = action_data.get('encoded_data')

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Setup complete')

    return hook