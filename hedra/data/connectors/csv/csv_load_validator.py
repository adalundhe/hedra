from pydantic import BaseModel, StrictStr
from typing import List, Optional


class CSVLoadValidator(BaseModel):
    headers: Optional[List[StrictStr]]