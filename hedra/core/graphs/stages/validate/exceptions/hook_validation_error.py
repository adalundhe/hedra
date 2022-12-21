from hedra.core.graphs.stages.base.stage import Stage


class HookValidationError(Exception):

    def __init__(self, from_stage: Stage, message: str) -> None:
        self.from_stage = from_stage
        self.to_stage = None

        super().__init__(
            f'Hook Validataion Error - Stage {from_stage.name} of type {from_stage.stage_type.name}:\n{message}'
        )