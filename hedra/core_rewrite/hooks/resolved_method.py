from pydantic import BaseModel, StrictStr

class ResolvedMethod(BaseModel):
    method: StrictStr