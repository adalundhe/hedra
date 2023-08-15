from pydantic import (
    BaseModel,
    StrictStr,
    StrictBool,
    StrictInt,
    StrictFloat
)

from typing import Union


class ExecuteStageSummaryValidator(BaseModel):
    stage: StrictStr
    total_elapsed: Union[StrictInt, StrictFloat]
    total_results: Union[StrictInt, StrictFloat]
    stage_batch_size: StrictInt
    stage_optimized: StrictBool
    stage_persona_type: StrictStr
    stage_workers: StrictInt