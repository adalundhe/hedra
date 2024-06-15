from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class TitleCommand(BaseModel):
    timeout: StrictInt | StrictFloat