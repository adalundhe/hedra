
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class FocusCommand(BaseModel):
    timeout: StrictInt | StrictFloat