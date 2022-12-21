import json
from typing import Dict, Any, Union
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.engine.action import Action
from hedra.logging import HedraLogger


async def assemble_plugin_action(
    hook: Union[ActionHook, TaskHook],
    hook_action: Dict[str, Any],
    persona: DefaultPersona,
    config: Config,
    metadata_string: str
):

    logger = HedraLogger()
    logger.initialize()

    action_name = hook_action.get('name')
    action_fields: Dict[str, Any] = hook_action.get('fields', {})

    action_hooks = hook_action.get('hooks', {})
    action_hook_names = action_hooks.get('names')

    before_hook_name = action_hook_names.get('before')
    after_hook_name = action_hook_names.get('after')
    check_hook_names = action_hook_names.get('checks', [])
    listener_names = action_hook_names.get('listeners', [])
    channel_hook_names = action_hook_names.get('channels', [])


    plugin_type: str = hook_action.get('plugin_type')
    plugin_type_name = plugin_type.capitalize()

    await logger.filesystem.aio['hedra.core'].debug(
        f'{metadata_string} - Assembling {plugin_type_name} Hook - {action_name}'
    )

    if before_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Found Before Hook - {before_hook_name}'
        )

    if after_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Found After Hook - {after_hook_name}'
        )

    for check_hook_name in check_hook_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Found Check Hook - {check_hook_name}'
        )

    for channel_hook_name in channel_hook_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Found Channel Hook - {channel_hook_name}'
        )

    for listener_name in listener_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Found Listener - {listener_name}'
        )

    timeouts: Timeouts = hook_action.get('timeouts', {})
    reset_connections = hook_action.get('reset_connections')

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Connect Timeout: {timeouts.connect_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Read Timeout: {timeouts.socket_read_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Total Timeout: {timeouts.total_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Reset Connections: {reset_connections}')

    plugin_config = Config(
        batch_size=persona.batch.size,
        connect_timeout=timeouts.connect_timeout,
        request_timeout=timeouts.total_timeout,
        reset_connections=reset_connections
    )

    plugin: EnginePlugin = registered_engines.get(plugin_type)(plugin_config)
    hook.session = plugin

    hook.action: Action = plugin.action(**{
        'name': action_name,
        **action_fields,
        **hook_action.get('metadata')
    })

    for field_name, field_value in action_fields.items():
        serialized_field = json.dumps({field_name: field_value})
        await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - {serialized_field}')

    hook.action.use_security_context = hook_action.get('use_security_context', False)
    hook.action.security_context = hook_action.get('security_context', False)
    hook.action.plugin_type = plugin_type

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Use Security Context: {hook.action.use_security_context}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - User: {hook.action.metadata.user}')

    for tag in hook.action.metadata.tags_to_string_list():
        await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Tag: {tag}')

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

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {plugin_type_name} Hook - {action_name} - Setup complete')

    return hook