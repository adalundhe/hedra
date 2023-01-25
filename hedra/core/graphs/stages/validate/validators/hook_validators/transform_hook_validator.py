from typing import Union
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.graphs.hooks.registry.registry_types import TransformHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class TransformHookVaidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            TransformHookVaidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: Union[TransformHook, BaseEvent]):

        try:
            
            call = hook._call
            if isinstance(hook, BaseEvent):
                call = hook.target._call

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.TRANSFORM, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.TRANSFORM.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @transform hook {hook.name}:{hook.hook_id}."
            assert hook.pre is not None, f'Pre argument for @transform Hook {hook.name}:{hook.hook_id} must be True or False'
            assert isinstance(hook.order, int), f'Order argument for @transform Hook {hook.name}:{hook.hook_id} must be an integer'
            assert call is not None, f"Method is not not found on stage or was not supplied to @transform hook - {hook.name}:{hook.hook_id}"
            assert 'self' in call.__code__.co_varnames

            assert call.__code__.co_argcount > 1, f"Missing required argument 'result' for @transform hook {hook.name}:{hook.hook_id}"
            assert call.__code__.co_argcount < 3, f"Too many args. - @transform hook {hook.name}:{hook.hook_id} only requires 'result' as an additional arg."

            for name in hook.names:
                hook_for_validation = self.hooks_by_name.get(name)

                assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @transform hook {hook.name}:{hook.hook_id}"
            
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            if hook.store:
                assert hook.load is not None, f'Load key must be set for @transform Hook {hook.name}:{hook.hook_id} is store argument is provided'

            if hook.load or hook.store:
                assert hook.context is not None, f"Context must be set for @transform Hook {hook.name}:{hook.hook_id} if store or load argument is provided"

        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))