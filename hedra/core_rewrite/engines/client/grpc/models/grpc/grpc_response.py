from __future__ import annotations

import binascii
from typing import Dict, Optional, TypeVar

from pydantic import BaseModel, StrictBytes, StrictFloat, StrictInt, StrictStr

from .metadata import Metadata
from .url_metadata import URLMetadata

T = TypeVar('T')

class GRPCResponse(BaseModel):
    metadata: Metadata
    url: URLMetadata
    status: Optional[StrictInt]=None
    status_message: Optional[StrictStr]=None
    content: StrictBytes=b''
    timings: Dict[StrictStr, StrictFloat]={}

    class Config:
        arbitrary_types_allowed=True

    def check_success(self) -> bool:
        return (
            self.status and self.status >= 200 and self.status < 300
        )
    
    @property
    def data(self):
        wire_msg = binascii.b2a_hex(self.content)

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]

        return binascii.a2b_hex(msg)

    def parse(self, protobuf):
        protobuf.ParseFromString(self.data)
        return protobuf

