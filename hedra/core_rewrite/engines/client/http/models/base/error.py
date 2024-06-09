from pydantic import BaseModel, StrictStr, StrictInt


class Error(BaseModel):
    host: StrictStr
    port: StrictInt
    error: StrictStr
