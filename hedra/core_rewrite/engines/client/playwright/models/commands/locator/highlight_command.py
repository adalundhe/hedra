from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class HighlightCommand(BaseModel):
    timeout: StrictInt | StrictFloat
    