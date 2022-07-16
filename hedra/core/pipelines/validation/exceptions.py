
from hedra.test.stages.types.stage_types import StageTypes


class PipelineValidationError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MissingDependentsError(PipelineValidationError):

    def __init__(self, stage_type: StageTypes, expected_stage_type: StageTypes, invalid_stage: object, ) -> None:

        stage_type = stage_type.name.capitalize()
        expected_stage_type = expected_stage_type.name.capitalize()

        super().__init__(
        f'''
        \n
        Error - the {stage_type} stage - {invalid_stage.__name__} - has no {expected_stage_type} stage in either direct or any previous stage's dependencies.
        \n
        '''
        )

class UnexpectedDependencyTypeError(PipelineValidationError):
    
    def __init__(
            self, 
            stage_type: StageTypes, 
            expected_stage_type: StageTypes, 
            invalid_stage: object, 
            invalid_dependency_stages: list
        ) -> None:

        stage_type = stage_type.name.capitalize()
        expected_stage_type = expected_stage_type.name.capitalize()
        invalid_dependency_stage_names = '\n\t'.join(invalid_dependency_stages)

        super().__init__(
        f'''
        \n
        Error - the {stage_type} stage - {invalid_stage.__name__} - has non-{expected_stage_type} stage dependencies:

        {invalid_dependency_stage_names}

        which are not allowed for {stage_type} stages.
        \n
        '''
)
