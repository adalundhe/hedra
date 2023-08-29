from pydantic import (
    StrictStr,
    StrictInt
)
from typing import Literal, Optional, Union, Tuple, List
from hedra.distributed.models.base.message import Message


HealthStatus = Literal[
    "initializing",
    "waiting",
    "healthy", 
    "suspect", 
    "failed"
]

class HealthCheck(Message):
    target_host: Optional[StrictStr]
    target_port: Optional[StrictInt]
    target_status: Optional[HealthStatus]
    target_last_updated: Optional[StrictInt]
    target_instance_id: Optional[Union[StrictInt, None]]
    registered_nodes: Optional[List[Tuple[StrictStr, StrictInt, StrictInt]]]
    registered_count: Optional[StrictInt]
    source_host: StrictStr
    source_port: StrictInt
    source_status: Optional[HealthStatus]
    status: HealthStatus