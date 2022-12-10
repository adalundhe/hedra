from typing import List


class InvalidStageType(Exception):

    def __init__(self, stage_type: str, valid_types: List[str]) -> None:

        valid_stage_types = '\n-'.join(valid_types)

        super().__init__(
            f'\n\nError - invalid stage type - {stage_type} - specified.\n\nValid types are: \n-{valid_stage_types}\n'
        )