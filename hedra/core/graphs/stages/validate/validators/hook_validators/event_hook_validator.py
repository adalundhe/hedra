from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class EventHookVaidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            EventHookVaidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: EventHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.EVENT, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.EVENT.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @event hook {hook.name}:{hook.hook_id}."
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to @event hook - {hook.name}:{hook.hook_id}"
            assert 'self' in hook.call.__code__.co_varnames

            if hook.pre is False and len(hook.names) > 0:
                assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'result' for @event hook {hook.name}:{hook.hook_id}"
                assert hook.call.__code__.co_argcount < 3, f"Too many args. - @event hook {hook.name}:{hook.hook_id} only requires 'result' as an additional arg."

            else:
                assert hook.call.__code__.co_argcount == 1, f"Too many args. - @event hook {hook.name}:{hook.hook_id} requires no additional args."

            for name in hook.names:
                hook_for_validation = self.hooks_by_name.get(name)

                assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @event hook {hook.name}:{hook.hook_id}"
            
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))