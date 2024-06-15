from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class DOMCommand(BaseModel):
    timeout: StrictInt | StrictFloat