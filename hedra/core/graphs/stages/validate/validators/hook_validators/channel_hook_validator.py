from typing import List, Union
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

    async def validate(self, hook: ChannelHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.CHANNEL, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.CHANNEL.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @channel hook {hook.name}:{hook.hook_id}."
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to @channel hook - {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount > 2, f"Missing required arguments 'result' or 'actions' for @channel hook {hook.name}:{hook.hook_id}"
            assert hook.call.__code__.co_argcount < 4, f"Too many args. - @channel hook {hook.name}:{hook.hook_id} only requires 'result' and 'actions' as additional args."
            assert 'self' in hook.call.__code__.co_varnames

            stage_actions: List[Union[ActionHook, TaskHook]] = list(
                filter(
                    lambda action_or_task: action_or_task.stage == hook.stage,
                    self.action_and_task_hooks
                )
            )

            stage_notifiers = []
            for action in stage_actions:
                stage_notifiers.extend(action.notifiers)

            stage_listeners = []
            for action in stage_actions:
                stage_listeners.extend(action.listeners)

            assert hook.shortname in stage_notifiers, f"No notifiers found. - @channel hook {hook.name}:{hook.hook_id} requires at least one action with a notify list containing its name"
            assert hook.shortname in stage_listeners, f"No listeners found. - @channel hook {hook.name}:{hook.hook_id} requires at least one action with a listeners list containing its name"

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')
        
        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))