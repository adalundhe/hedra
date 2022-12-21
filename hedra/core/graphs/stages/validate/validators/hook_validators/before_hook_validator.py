from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import BeforeHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class BeforeHookVaidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            BeforeHookVaidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: BeforeHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.BEFORE, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.BEFORE.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @before hook {hook.name}:{hook.hook_id}."
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount > 2, f"Missing required argument 'action' for @before hook {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount < 4, f"Too many args. - @before hook {hook.name}:{hook.hook_id} only requires 'action' as additional arg."
            assert len(hook.names) > 0, f"No target hook names provided for @before hook {hook.name}:{hook.hook_id}. Please specify at least one hook to validate."
            assert 'self' in hook.call.__code__.co_varnames

            for name in hook.names:
                hook_for_validation = self.hooks_by_name.get(
                    f'{hook.stage}.{name}'
                )

                assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @before hook {hook.name}:{hook.hook_id}"
            
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))