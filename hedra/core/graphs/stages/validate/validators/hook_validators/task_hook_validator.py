from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import TaskHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class TaskHookValidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            TaskHookValidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: TaskHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.TASK, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.TASK.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @task hook {hook.name}:{hook.hook_id}."
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to @task hook - {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount == 1, f"Too many args. - @task hook {hook.name}:{hook.hook_id} requires no additional args."
            assert 'self' in hook.call.__code__.co_varnames

            for notify_action in hook.notifiers:
                assert notify_action not in hook.listeners, f"Notify/listen loopback. - @task hook {hook.name}:{hook.hook_id} cannot notify and listen on same channel."

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')


        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))