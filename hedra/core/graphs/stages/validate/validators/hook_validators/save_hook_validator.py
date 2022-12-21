import os
from pathlib import Path
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import SaveHook
from hedra.core.graphs.stages.validate.exceptions import HookValidationError
from .base_hook_validator import BaseHookVaidator


class SaveHookValidator(BaseHookVaidator):

    def __init__(self, metadata_string: str) -> None:
        super(
            SaveHookValidator,
            self
        ).__init__(metadata_string)

    async def validate(self, hook: SaveHook):

        try:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validating {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

            assert hook.hook_type is HookType.SAVE, f"Hook type mismatch - hook {hook.name}:{hook.hook_id} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.SAVE.name} hook."
            assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name}:{hook.hook_id} for @save hook {hook.name}:{hook.hook_id}."
            
            assert hook.call is not None, f"Method is not not found on stage or was not supplied to @save hook - {hook.name}:{hook.hook_id}"
            assert 'self' in hook.call.__code__.co_varnames

            if hook.save_path:
                hook_path_dir = str(Path(hook.save_path).parent.resolve())
                
                assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'data' for @save hook {hook.name}:{hook.hook_id}"
                assert hook.call.__code__.co_argcount < 3, f"Too many args. - @save hook {hook.name}:{hook.hook_id} only requires 'data' as additional args."
                assert hook.context_key is not None, f"Missing required keyword arg. - @save hook {hook.name}:{hook.hook_id} requires a valid string for 'key'"
                assert hook.save_path is not None, f"Missing required keyword arg. - @save hook {hook.name}:{hook.hook_id} requires a valid string for 'checkpoint_filepath'"
                assert isinstance(hook.save_path, str), f"Invalid path type - @save hook {hook.name}:{hook.hook_id} path must be a valid string."
                assert os.path.exists(hook_path_dir), f"Invalid path - @save hook {hook.name}:{hook.hook_id} path {hook_path_dir} must exist."
            
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Validated {hook.hook_type.name.capitalize()} Hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

        except AssertionError as hook_validation_error:
                raise HookValidationError(hook.stage_instance, str(hook_validation_error))