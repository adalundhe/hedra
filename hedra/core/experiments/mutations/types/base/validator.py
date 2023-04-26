from pydantic import (
    BaseModel, 
    StrictFloat, 
    StrictStr,
    validator
)
from typing import Tuple


class MutationValidator(BaseModel):
    name: StrictStr
    chance: StrictFloat
    targets: Tuple[StrictStr, ...]

    @validator('targets')
    def validate_targets(cls, val):
        assert len(val) > 0, "Mutations must target more than one Action or Task hook."
        return val