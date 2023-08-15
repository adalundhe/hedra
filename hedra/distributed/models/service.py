from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    IPvAnyAddress
)

from typing import (
    Dict, 
    Tuple,
    Literal
)


class Service(BaseModel):
    service_instance: StrictStr
    service_name: StrictStr
    service_protocol: Literal["udp", "tcp"]
    service_url: StrictStr
    service_ip: IPvAnyAddress
    service_port: StrictInt
    service_context: Dict[StrictStr, StrictStr]={}

    def to_address(self) -> Tuple[str, int]:
        return (
            str(self.service_ip),
            self.service_port
        )