from pydantic import (
    BaseModel,
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
    MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL: StrictStr='1s'
   
    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT': str,
            'MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL': str,
            'MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL': str
        }