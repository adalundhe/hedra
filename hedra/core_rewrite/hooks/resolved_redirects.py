from pydantic import BaseModel, StrictInt


class ResolvedRedirects(BaseModel):
    redirects: StrictInt
