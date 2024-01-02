from pydantic import BaseModel, StrictBytes, StrictStr


class ResolvedAuth(BaseModel):
    username: StrictStr
    password: StrictStr
    auth: StrictBytes