from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr
)
from typing import (
    Dict, 
    Union,
    Callable
)


PrimaryType = Union[str, int, float, bytes, bool]


class ReplicationEnv(BaseModel):
    MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT: StrictStr='30s'
    MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL: StrictStr='1s'
    MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL: StrictStr='1s'
    MERCURY_SYNC_RAFT_REGISTRATION_TIMEOUT: StrictStr='15s'
    MERCURY_SYNC_RAFT_EXPECTED_NODES: StrictInt=3
    MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_AGE: StrictStr='1h'
    MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_COUNT: StrictInt=1000
   
    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT': str,
            'MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL': str,
            'MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL': str,
            'MERCURY_SYNC_RAFT_REGISTRATION_TIMEOUT': str,
            'MERCURY_SYNC_RAFT_EXPECTED_NODES': int,
            'MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_AGE': str,
            'MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_COUNT': int
        }