from pydantic import BaseModel, Field


class MetricVaidator(BaseModel):
    group: str=Field(..., min_length=1)

    def __init__(__pydantic_self__,  group: str) -> None:
        super().__init__(group=group)
