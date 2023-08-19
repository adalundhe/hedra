from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)

class Entry(BaseModel):
    entry_id: StrictInt
    key: StrictStr
    value: StrictStr
    term: StrictInt
    leader_host: StrictStr
    leader_port: StrictInt
