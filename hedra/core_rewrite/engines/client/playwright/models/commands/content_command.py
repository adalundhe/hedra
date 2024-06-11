
from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class ContentCommand(BaseModel):
    timeout: StrictInt | StrictFloat
    
    class Config:
        arbitrary_types_allowed=True
        