from hedra.distributed.models.base.message import Message
from pydantic import (
    StrictStr,
    StrictInt
)
from typing import List, Optional, Tuple
from .vote_result import VoteResult
from .healthcheck import HealthStatus
from .logs import Entry, NodeState


class RaftMessage(Message):
    source_host: StrictStr
    source_port: StrictInt
    elected_leader: Optional[Tuple[StrictStr, StrictInt]]
    failed_node: Optional[Tuple[StrictStr, StrictInt]]
    vote_result: Optional[VoteResult]
    raft_node_status: NodeState
    status: HealthStatus
    entries: Optional[List[Entry]]
    term_number: StrictInt
    received_timestamp: Optional[StrictInt]
