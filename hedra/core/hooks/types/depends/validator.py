from typing import Any, Tuple
from hedra.core.graphs.stages.base.stage import Stage
from pydantic import BaseModel, validator


class DependsValidator(BaseModel):
    stages: Tuple[Any, ...]

    class Config:
        arbitrary_types_allowed = True

    @validator('stages')
    def validate_stages(cls, vals):
        assert len(vals) > 0
        assert len(vals) == len(set(vals))

        for val in vals:
            assert issubclass(val, Stage)

        return vals
