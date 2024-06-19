from pydantic import BaseModel, StrictStr


class QueryValidator(BaseModel):
    value: StrictStr
