from pydantic import BaseModel, StrictStr, StrictInt
from typing import Dict, Any, Optional


class CassandraLoadValidator(BaseModel):
    fields: Dict[StrictStr, Any]
    filters: Optional[Dict[StrictStr, Any]]
    limit: Optional[StrictInt]