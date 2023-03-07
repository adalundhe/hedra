from typing import List, Union
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook,
    ChannelHook
)
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class ChannelHookValidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            ChannelHookValidator,
            self
        ).__init__(metadata_string)

        self.action_and_task_hooks: List[Union[ActionHook, TaskHook]] = []

    async def validate(self, hook: Union[ChannelHook, BaseEvent]):

        try:

            call = hook._call
            if isinstance(hook, BaseEvent):
                call = hook.target._call

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.CHANNEL, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.CHANNEL.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @channel hook {hook.name}:{hook.hook_id}."
            assert call is not None, f"Method is not not found on stage or was not supplied to @channel hook - {hook.name}:{hook.hook_id}"
            assert 'self' in call.__code__.co_varnames

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')
        
        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))