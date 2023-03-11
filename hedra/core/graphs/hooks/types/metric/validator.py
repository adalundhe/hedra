from pydantic import BaseModel, Field, StrictStr


class MetricHookValidator(BaseModel):
    group: StrictStr=Field(..., min_length=1)
