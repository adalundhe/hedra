from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType


class HookSetupTimeoutError(Exception):

    def __init__(self, hook: Hook, hook_type: HookType, timeout: float) -> None:

        hook_type = hook_type.name.lower()
        super().__init__(
            f'Hook Error - @{hook_type} hook {hook.shortname} from stage {hook.stage}\nHook failed to complete setup in specified connection timeout of - {timeout} - seconds.'
        )
