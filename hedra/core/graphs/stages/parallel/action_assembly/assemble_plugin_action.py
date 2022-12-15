from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.client.config import Config
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.engine.action import Action
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona


def assemble_plugin_action(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona
):


    action_hooks = hook_action.get('hooks', {})

    before_hook_name = action_hooks.get('before')
    after_hook_name = action_hooks.get('after')
    check_hook_names = action_hooks.get('checks')

    plugin_type = hook_action.get('plugin_type')
    timeouts: Timeouts = hook_action.get('timeouts', {})


    config = Config(
        batch_size=persona.batch.size,
        connect_timeout=timeouts.connect_timeout,
        request_timeout=timeouts.total_timeout,
        reset_connections=hook_action.get('reset_connections')
    )

    plugin: EnginePlugin = registered_engines.get(plugin_type)(config)
    hook.session = plugin

    hook.action: Action = plugin.action(**{
        'name': hook_action.get('name'),
        **hook_action.get('fields', {}),
        **hook_action.get('metadata')
    })

    hook.action.use_security_context = hook_action.get('use_security_context', False)
    hook.action.security_context = hook_action.get('security_context', False)
    hook.action.plugin_type = plugin_type

    if before_hook_name:
        before_hook = registrar.all.get(before_hook_name)
        hook.action.hooks.before = before_hook.call

    if after_hook_name:
        after_hook = registrar.all.get(after_hook_name)
        hook.action.hooks.after = after_hook.call

    hook.action.hooks.checks = []
    for check_hook_name in check_hook_names:
        hook.action.hooks.checks.append(check_hook_name)

    return hook