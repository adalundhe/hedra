from pydantic import BaseModel, StrictBytes, StrictStr


class ResolvedParams(BaseModel):
    params: StrictStr | StrictBytes
