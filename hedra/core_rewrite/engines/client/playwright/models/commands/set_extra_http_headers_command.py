from typing import Dict

from pydantic import BaseModel, StrictFloat, StrictInt, StrictStr


class SetExtraHTTPHeadersCommand(BaseModel):
    headers: Dict[StrictStr, StrictStr]
    timeout: StrictInt | StrictFloat