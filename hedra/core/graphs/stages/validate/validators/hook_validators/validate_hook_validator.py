from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import ValidateHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class ValidateHookValidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            ValidateHookValidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: ValidateHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.VALIDATE, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.VALIDATE.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @validate hook {hook.name}:{hook.hook_id}."
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'hook' for @validate hook {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount < 3, f"Too many args. - @validate hook {hook.name}:{hook.hook_id} only requires 'events_group' as additional arg."
            assert hook.stage is not None, f"No target stage name provided for @validate hook {hook.name}:{hook.hook_id}. Please specify the stage name whose hooks you wish to validate."
            assert len(hook.names) > 0, f"No target hook names provided for @validate hook {hook.name}:{hook.hook_id}. Please specify at least one hook to validate."
            assert 'self' in hook.call.__code__.co_varnames

            for name in hook.names:
                hook_for_validation = self.hooks_by_name.get(name)
                stage, hook_name = name.split('.')

                assert hook_for_validation is not None, f"Specified hook {hook_name} for stage {stage} not found for @validate hook {hook.name}:{hook.hook_id}"

                await hook.call(hook_for_validation.call)

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            
        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))