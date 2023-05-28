from pydantic import BaseModel, StrictStr, StrictInt
from typing import Dict, Any, Optional

class FieldOption:
    field_type: StrictStr
    options: Dict[StrictStr, Any]


class CassandraLoadValidator(BaseModel):
    fields: Optional[Dict[StrictStr, FieldOption]]
    filters: Optional[Dict[StrictStr, Any]]
    limit: Optional[StrictInt]