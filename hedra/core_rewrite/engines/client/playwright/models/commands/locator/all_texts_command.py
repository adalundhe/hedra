from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class AllTextsCommand(BaseModel):
    timeout: StrictInt | StrictFloat
    