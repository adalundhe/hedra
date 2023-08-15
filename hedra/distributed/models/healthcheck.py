from pydantic import (
    StrictStr,
    StrictInt
)
from typing import Literal, Optional
from .message import Message


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
    source_host: StrictStr
    source_port: StrictInt
    source_status: Optional[HealthStatus]
    status: HealthStatus