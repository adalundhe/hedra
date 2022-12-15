from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.task import Task, MercuryTaskRunner
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes
from hedra.logging import HedraLogger


async def assemble_task(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona,
    config: Config,
    metadata_string: str
):

    logger = HedraLogger()
    logger.initialize()

    hook_type_name = RequestTypes.TASK.capitalize()
    task_name = hook_action.get('name')
    task_hooks = hook_action.get('hooks', {})
    
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Assembling {hook_type_name} Hook - {task_name}')

    before_hook_name = task_hooks.get('before')
    after_hook_name = task_hooks.get('after')
    check_hook_names = task_hooks.get('checks', [])

    if before_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {task_name} - Found Before Hook - {before_hook_name}'
        )

    if after_hook_name:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {task_name} - Found After Hook - {after_hook_name}'
        )

    for check_hook_name in check_hook_names:
        await logger.filesystem.aio['hedra.core'].debug(
            f'{metadata_string} - {hook_type_name} Hook - {task_name} - Found Check Hook - {check_hook_name}'
        )
    

    hook.session: MercuryTaskRunner = registered_engines.get(RequestTypes.TASK)(
        concurrency=persona.batch.size,
        timeouts=hook_action.get('timeouts')
    )

    task_hook = registrar.all.get(task_name)

    hook.action: Task = Task(
        task_name,
        task_hook.call,
        source=hook.config.env,
        user=hook.config.user,
        tags=hook.config.tags
    )


    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {task_name} - Env: {hook.action.source}')
    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {task_name} - User: {hook.action.metadata.user}')

    for tag in hook.action.metadata.tags_to_string_list():
        await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {task_name} - Tag: {tag}')

    if before_hook_name:
        before_hook = registrar.all.get(before_hook_name)
        hook.action.hooks.before = before_hook.call

    if after_hook_name:
        after_hook = registrar.all.get(after_hook_name)
        hook.action.hooks.after = after_hook.call

    hook.action.hooks.checks = []
    for check_hook_name in check_hook_names:
        hook.action.hooks.checks.append(check_hook_name)

    await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - {hook_type_name} Hook - {task_name} - Setup complete')

    return hook