from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.stage import Stage


class HookValidationError(Exception):

    def __init__(self, from_stage: Stage, message: str) -> None:
        self.from_stage = from_stage
        self.to_stage = None

        super().__init__(
            f'Hook Validataion Error - Stage {from_stage.name} of type {from_stage.stage_type.name}:\n{message}'
        )


class ReservedMethodError(Exception):

    def __init__(self, from_stage: Stage, reserve_method_name: str) -> None:
        self.from_stage = from_stage
        self.to_stage = None

        super().__init__(
            f'Stage Validation Error - Stage {from_stage.name} of type {from_stage.stage_type.name}:\nThe class method name - {reserve_method_name} - is reserved by Hedra. Please choose a different method name.'
        )


class MissingReservedMethodError(Exception):

    def __init__(self, from_stage: Stage, reserve_method_name: str) -> None:
        self.from_stage = from_stage
        self.to_stage = None

        super().__init__(
            f'Stage Validation Error - Stage {from_stage.name} of type {from_stage.stage_type.name}:\nThe class method name - {reserve_method_name} - is reserved and required by Hedra but was not found on the Stage.'
        )


class HookSetupError(Exception):

    def __init__(self, hook: Hook, hook_type: HookType, message: str) -> None:

        hook_type = hook_type.name.lower()
        super().__init__(
            f'Hook Error - @{hook_type} hook {hook.shortname} from stage {hook.stage}\nEncountered exception - {message} - while attempting to setup hook.'
        )


class HookSetupTimeoutError(Exception):

    def __init__(self, hook: Hook, hook_type: HookType, timeout: float) -> None:

        hook_type = hook_type.name.lower()
        super().__init__(
            f'Hook Error - @{hook_type} hook {hook.shortname} from stage {hook.stage}\nHook failed to complete setup in specified connection timeout of - {timeout} - seconds.'
        )
