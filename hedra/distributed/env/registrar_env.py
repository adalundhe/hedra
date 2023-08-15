from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from typing import (
    Dict, 
    Union,
    Callable,
    Literal
)


PrimaryType = Union[str, int, float, bytes, bool]


class RegistrarEnv(BaseModel):
    MERCURY_SYNC_REGISTRAR_CLIENT_POLL_RATE: StrictStr='1s'
    MERCURY_SYNC_REGISTRAR_EXPECTED_NODES: StrictInt
    MERCURY_SYNC_REGISTRATION_TIMEOUT: StrictStr='1m'
    MERCURY_SYNC_RESOLVER_CONNECTION_TYPE: Literal["udp", "tcp", "http"]="udp"
    MERCURY_SYNC_RESOLVER_REQUEST_TIMEOUT: StrictStr='5s'
    MERCURY_SYNC_RESOLVER_MAXIMUM_TRIES: StrictInt=5

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_REGISTRAR_CLIENT_POLL_RATE': str,
            'MERCURY_SYNC_REGISTRAR_EXPECTED_NODES': int,
            'MERCURY_SYNC_REGISTRATION_TIMEOUT': str,
            'MERCURY_SYNC_RESOLVER_CONNECTION_TYPE': str,
            'MERCURY_SYNC_RESOLVER_REQUEST_TIMEOUT': str,
            'MERCURY_SYNC_RESOLVER_MAXIMUM_TRIES': int
        }