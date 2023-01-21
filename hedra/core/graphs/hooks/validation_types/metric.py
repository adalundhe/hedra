from pydantic import BaseModel, Field, StrictStr


class MetricHookValidator(BaseModel):
    group: StrictStr=Field(..., min_length=1)


class MetricValidator:

    def __init__(__pydantic_self__,  group: str) -> None:
        MetricHookValidator(
            group=group
        )
