from pydantic import BaseModel, StrictStr, StrictInt
from typing import Dict, Any, Optional, Type

class FieldOption:
    field_type: StrictStr
    options: Dict[StrictStr, Any]


class CassandraLoadValidator(BaseModel):
    fields: Optional[Dict[StrictStr, FieldOption]]
    filters: Optional[Dict[StrictStr, Any]]
    table: Type[Any]
    limit: Optional[StrictInt]

    class Config:
        arbitrary_types_allowed=True