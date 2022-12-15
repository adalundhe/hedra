from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.graphql import GraphQLAction
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes


def assemble_graphql_action(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona
):
    
    action_name = hook_action.get('name')
    action_hooks = hook_action.get('hooks', {})

    before_hook_name = action_hooks.get('before')
    after_hook_name = action_hooks.get('after')
    check_hook_names = action_hooks.get('checks')

    hook.session = registered_engines.get(RequestTypes.GRAPHQL)(
        concurrency=persona.batch.size,
        timeouts=hook_action.get('timeouts'),
        reset_connections=hook_action.get('reset_connections')
    )

    action_url = hook_action.get('url', {})
    action_headers = hook_action.get('headers', {})
    action_data = hook_action.get('data', {})
    action_metadata = hook_action.get('metadata', {})

    hook.action = GraphQLAction(
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

    return hook