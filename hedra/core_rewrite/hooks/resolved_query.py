from pydantic import BaseModel, StrictBytes, StrictInt


class ResolvedQuery(BaseModel):
    query: StrictBytes
    size: StrictInt