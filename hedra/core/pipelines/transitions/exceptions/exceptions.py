from hedra.core.pipelines.stages.stage import Stage


class InvalidTransitionError(Exception):
    
    def __init__(self, from_stage: Stage, to_stage: Stage) -> None:

        from_stage_name = from_stage.__class__.__name__
        to_stage_name = to_stage.__class__.__name__
        from_stage_type = from_stage.stage_type.name.capitalize()
        to_stage_type = to_stage.stage_type.name.capitalize()

        super().__init__(
            f'Error - {from_stage_type} stage {from_stage_name} cannot preceed {to_stage_type} stage {to_stage_name}.'
        )


class IsolatedStageError(Exception):

    def __init__(self, stage: Stage) -> None:

        stage_name = stage.__class__.__name__
        stage_type = stage.stage_type.name.capitalize()

        super().__init__(
            f'Error - {stage_type} stage {stage_name} has no dependencies and is thus unreachable by the execution graph.'
        )