from pydantic import BaseModel, StrictInt


class NthCommand(BaseModel):
    index: StrictInt