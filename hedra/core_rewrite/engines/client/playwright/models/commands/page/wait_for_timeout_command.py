from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class WaitForTimeout(BaseModel):
    timeout: StrictInt | StrictFloat