from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.task import Task
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes


def assemble_task(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona
):

    task_hooks = hook_action.get('hooks', {})

    before_hook_name = task_hooks.get('before')
    after_hook_name = task_hooks.get('after')
    check_hook_names = task_hooks.get('checks')

    hook.session = registered_engines.get(RequestTypes.TASK)(
        concurrency=persona.batch.size,
        timeouts=hook_action.get('timeouts')
    )

    task_name = hook_action.get('name')
    task_hook = registrar.all.get(task_name)

    hook.action: Task = Task(
        task_name,
        task_hook.call,
        source=hook.config.env,
        user=hook.config.user,
        tags=hook.config.tags
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

    return hook