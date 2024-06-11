
from pydantic import BaseModel, StrictFloat, StrictInt


class GetUrlCommand(BaseModel):
        timeout: StrictInt | StrictFloat

