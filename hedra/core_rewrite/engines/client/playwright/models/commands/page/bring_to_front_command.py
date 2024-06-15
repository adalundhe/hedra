from pydantic import BaseModel, StrictFloat, StrictInt


class BringToFrontCommand(BaseModel):
    timeout: StrictInt | StrictFloat