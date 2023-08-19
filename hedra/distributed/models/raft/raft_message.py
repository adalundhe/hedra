from hedra.distributed.models.base.message import Message
from pydantic import (
    StrictStr,
    StrictInt
)
from typing import List, Optional, Tuple
from .election_state import ElectionState
from .logs import Entry, NodeState


class RaftMessage(Message):
    source_host: StrictStr
    source_port: StrictInt
    elected_leader: Optional[Tuple[StrictStr, StrictInt]]
    election_status: ElectionState
    raft_node_status: NodeState
    entries: Optional[List[Entry]]
    term_number: Optional[StrictInt]

