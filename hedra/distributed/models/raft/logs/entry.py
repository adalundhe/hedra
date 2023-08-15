from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt, 
    Json
)

class Entry(BaseModel):
    entry_id: StrictInt
    key: StrictStr
    value: Json
    term: StrictInt
