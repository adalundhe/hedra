from hedra.core.engines.types.common.protocols.ping.ping_type import PingTypesMap
from pydantic import (
    BaseModel, 
    StrictStr,
    StrictInt,
    validator
)


class InjectPingValidator(BaseModel):
    ping_type: StrictStr
    timeout: StrictInt

    @validator('ping_type')
    def validate_ping_type(cls, val):
        types_map = PingTypesMap()
        assert types_map.get(val) is not None
        
        return val

