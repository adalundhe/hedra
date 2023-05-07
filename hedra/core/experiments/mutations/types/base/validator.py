from pydantic import (
    BaseModel, 
    StrictFloat, 
    StrictStr,
    validator
)
from typing import Tuple
from .mutation_type import MutationType


class MutationValidator(BaseModel):
    name: StrictStr
    chance: StrictFloat
    targets: Tuple[StrictStr, ...]
    mutation_type: MutationType

    class Config:
        arbitrary_types_allowed=True

    @validator('targets')
    def validate_targets(cls, val):
        assert len(val) > 0, "Mutations must target more than one Action or Task hook."
        return val