from typing import Literal, Optional, Union

import orjson
from pydantic import BaseModel, StrictBytes, StrictStr


class UDPRequest(BaseModel):
    url: StrictStr
    method: Literal[
        "BIDIRECTIONAL", 
        "SEND",
        "RECEIVE",
    ]
    data: Union[
        StrictStr,
        StrictBytes,
        BaseModel,
    ]=b''
    response_size: Optional[int]=None
    delimiter: StrictBytes=b'\n'

    class Config:
        arbitrary_types_allowed=True

    def prepare(self):

        if isinstance(self.data, BaseModel):
            data = orjson.dumps({
                name: value for name, value in self.data.__dict__.items() if value is not None
            })

        elif isinstance(self.data, str):
            data = self.data.encode()

        elif self.data:
            data = self.data

        else:
            data = self.data

        return data
