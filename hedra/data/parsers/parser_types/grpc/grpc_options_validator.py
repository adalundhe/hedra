from pydantic import BaseModel, StrictStr
from typing import Dict, Any, Callable


class GRPCOptionsValidator(BaseModel):
    protobuf_map: Dict[StrictStr, Callable[[Dict[str, Any]], object]]