import json
from typing import Dict, Any
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
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes
from hedra.logging import HedraLogger


async def assemble_playwright_command(
    hook: Hook,
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

    before_hook_name = action_hooks.get('before')
    after_hook_name = action_hooks.get('after')
    check_hook_names = action_hooks.get('checks', [])

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

    for field_name, field_value in hook.action.to_serializable():
        if field_value is not None:
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - {field_name.capitalize()}: {field_value}')

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

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {action_name} - Setup complete')

    return hook