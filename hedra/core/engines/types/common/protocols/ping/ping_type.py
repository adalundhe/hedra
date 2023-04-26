from enum import Enum


class PingType(Enum):
    ICMP='ICMP'
    TCP='TCP'
    UDP='UDP'


class PingTypesMap:
    
    def __init__(self) -> None:
        self._ping_types = {
            'icmp': PingType.ICMP,
            'tcp': PingType.TCP,
            'udp': PingType.UDP
        }

        self._ping_type_name = {
            PingType.ICMP: 'icmp',
            PingType.TCP: 'tcp',
            PingType.UDP: 'udp'
        }

    def get(self, ping_type_name: str) -> PingType:
        return self._ping_types.get(ping_type_name)
    
    def get_name(self, ping_type: PingType) -> str:
        return self._ping_type_name.get(ping_type)