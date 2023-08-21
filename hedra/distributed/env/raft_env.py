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


class RaftEnv(BaseModel):
    MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT: StrictStr='30s'
    MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL: StrictStr='1s'
    MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL: StrictStr='5s'
    MERCURY_SYNC_RAFT_REGISTRATION_TIMEOUT: StrictStr='15s'
    MERCURY_SYNC_RAFT_EXPECTED_NODES: StrictInt=3
   
    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT': str,
            'MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL': str,
            'MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL': str,
            'MERCURY_SYNC_RAFT_REGISTRATION_TIMEOUT': str,
            'MERCURY_SYNC_RAFT_EXPECTED_NODES': int
        }