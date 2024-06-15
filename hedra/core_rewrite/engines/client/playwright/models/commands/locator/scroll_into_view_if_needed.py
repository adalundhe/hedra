from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ScrollIntoViewIfNeeded(BaseModel):
    timeout: StrictInt | StrictFloat