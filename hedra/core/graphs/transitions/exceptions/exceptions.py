from hedra.core.graphs.stages.base.stage import Stage


class InvalidTransitionError(Exception):
    
    def __init__(self, from_stage: Stage, to_stage: Stage) -> None:

        from_stage_name = from_stage.__class__.__name__
        to_stage_name = to_stage.__class__.__name__
        from_stage_type = from_stage.stage_type.name.capitalize()
        to_stage_type = to_stage.stage_type.name.capitalize()

        super().__init__(
            f'Error - {from_stage_type} stage {from_stage_name} cannot preceed {to_stage_type} stage {to_stage_name}.'
        )

class IdleTranstionError(Exception):
    def __init__(self, stage: Stage) -> None:

        stage_name = stage.__class__.__name__
        stage_type = stage.stage_type.name.capitalize()

        super().__init__(
            f"Error - {stage_type} stage {stage_name} cannot be the initial stage. Make sure you have a Setup class and set it as {stage_name}'s dependency!"
        )

class IsolatedStageError(Exception):

    def __init__(self, stage: Stage) -> None:

        stage_name = stage.__class__.__name__
        stage_type = stage.stage_type.name.capitalize()

        super().__init__(
            f'Error - {stage_type} stage {stage_name} has no dependencies and is thus unreachable by the execution graph.'
        )


class StageTimeoutError(Exception):

    def __init__(self, from_stage: Stage) -> None:
        self.from_stage = from_stage

        super().__init__(
            f'Stage Timeout Exception - Stage {from_stage.name} of type {from_stage.stage_type.name}:\nStage exceeded provided timeout of {from_stage.timeout} seconds.'
        )


class StageExecutionError(Exception):

    def __init__(self, from_stage: Stage, to_stage: Stage, message: str) -> None:

        self.from_stage = from_stage
        self.to_stage = to_stage

        super().__init__(
            f'Stage Execution Exception - Stage {from_stage.name} of type {from_stage.stage_type.name}:\n Encountered error - {message} - while transitioning to {to_stage.stage_type.name} stage - {self.to_stage.name}.'
        )