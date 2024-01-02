from pydantic import BaseModel, StrictBytes


class ResolvedParams(BaseModel):
    params: StrictBytes