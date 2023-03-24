from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType


class HookSetupError(Exception):

    def __init__(self, hook: Hook, hook_type: HookType, message: str) -> None:

        hook_type = hook_type.name.lower()
        super().__init__(
            f'Hook Error - @{hook_type} hook {hook.shortname} from stage {hook.stage}\nEncountered exception - {message} - while attempting to setup hook.'
        )

