import json
from typing import Dict, Any, Union
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.playwright import (
    PlaywrightCommand,
    Page,
    Input,
    URL,
    Options,
    MercuryPlaywrightClient
)
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes
from hedra.logging import HedraLogger


async def assemble_playwright_command(
    hook: Union[ActionHook, TaskHook],
    hook_action: Dict[str, Any],
    persona: DefaultPersona,
    config: Config,
    metadata_string: str
):
    logger = HedraLogger()
    logger.initialize()

    action_name = hook_action.get('name')
    hook_type_name = RequestTypes.PLAYWRIGHT.capitalize()

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

    hook.session: MercuryPlaywrightClient = registered_engines.get(RequestTypes.PLAYWRIGHT)(
        concurrency=persona.batch.size,
        group_size=config.group_size,
        timeouts=hook_action.get('timeouts')
    )

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Connect Timeout: {hook.session.timeouts.connect_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Read Timeout: {hook.session.timeouts.socket_read_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Total Timeout: {hook.session.timeouts.total_timeout}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Group Size: {hook.session.pool.group_size}')

    action_metadata = hook_action.get('metadata', {})

    hook.action: PlaywrightCommand = PlaywrightCommand(
        action_name,
        hook_action.get('command'),
        page=Page(
            **hook_action.get('page', {})
        ),
        url=URL(
            **hook_action.get('url', {})
        ),
        input=Input(
            **hook_action.get('input', {})
        ),
        options=Options(
            **hook_action.get('options', {})
        ),
        user=action_metadata.get('user'),
        tags=action_metadata.get('tags', [])
    )

    for field_name, field_value in hook.action.iter_values():
        if field_value is not None:
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - {field_name.capitalize()}: {field_value}')

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

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Setup complete')

    return hook