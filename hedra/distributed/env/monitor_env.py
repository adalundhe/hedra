from ipaddress import IPv4Address
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat
)
from typing import (
    Dict, 
    Union,
    Callable
)


PrimaryType = Union[str, int, float, bytes, bool]


class MonitorEnv(BaseModel):
    MERCURY_SYNC_UDP_SYNC_INTERVAL: StrictStr='5s'
    MERCURY_SYNC_BOOT_WAIT: StrictStr='3s'
    MERCURY_SYNC_MAX_TIME_IDLE: StrictStr='10s'
    MERCURY_SYNC_IDLE_REBOOT_TIMEOUT: StrictStr='10s'
    MERCURY_SYNC_POLL_RETRIES: StrictInt=3
    MERCURY_SYNC_MIN_SUSPECT_NODES_THRESHOLD=3
    MERCURY_SYNC_MAX_POLL_MULTIPLIER: StrictInt=5
    MERCURY_SYNC_MIN_SUSPECT_TIMEOUT_MULTIPLIER: StrictInt=4
    MERCURY_SYNC_MAX_SUSPECT_TIMEOUT_MULTIPLIER: StrictInt=7
    MERCURY_SYNC_INITIAL_NODES_COUNT: StrictInt=3
    MERCURY_SYNC_HEALTH_CHECK_TIMEOUT: StrictStr='0.5s'
    MERCURY_SYNC_REGISTRATION_TIMEOUT: StrictStr='1m'
    MERCURY_SYNC_HEALTH_POLL_INTERVAL: StrictFloat='1s'
    MERCURY_SYNC_INDIRECT_CHECK_NODES: StrictInt=3

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_UDP_SYNC_INTERVAL': str,
            'MERCURY_SYNC_POLL_RETRIES': int,
            'MERCURY_SYNC_MAX_POLL_MULTIPLIER': int,
            'MERCURY_SYNC_MAX_TIME_IDLE': str,
            'MERCURY_SYNC_IDLE_REBOOT_TIMEOUT': str,
            'MERCURY_SYNC_MIN_SUSPECT_NODES_THRESHOLD': int,
            'MERCURY_SYNC_MIN_SUSPECT_TIMEOUT_MULTIPLIER': int,
            'MERCURY_SYNC_MAX_SUSPECT_TIMEOUT_MULTIPLIER': int,
            'MERCURY_SYNC_INITIAL_NODES_COUNT': int,
            'MERCURY_SYNC_BOOT_WAIT': str,
            'MERCURY_SYNC_REGISTRATION_TIMEOUT': str,
            'MERCURY_SYNC_HEALTH_POLL_INTERVAL': str,
            'MERCURY_SYNC_INDIRECT_CHECK_NODES': int
        }