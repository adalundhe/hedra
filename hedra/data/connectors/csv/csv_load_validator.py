from pydantic import BaseModel, StrictStr
from typing import List


class CSVLoadValidator(BaseModel):
    headers: List[StrictStr]