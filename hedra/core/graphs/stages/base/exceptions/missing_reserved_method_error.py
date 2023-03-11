from hedra.core.graphs.stages.base.stage import Stage


class MissingReservedMethodError(Exception):

    def __init__(self, from_stage: Stage, reserve_method_name: str) -> None:
        self.from_stage = from_stage
        self.to_stage = None

        super().__init__(
            f'Stage Validation Error - Stage {from_stage.name} of type {from_stage.stage_type.name}:\nThe class method name - {reserve_method_name} - is reserved and required by Hedra but was not found on the Stage.'
        )