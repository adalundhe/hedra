from __future__ import annotations
from pydantic import BaseModel, StrictStr, StrictInt
from typing import Optional

class Message(BaseModel):
    host: Optional[StrictStr]
    port: Optional[StrictInt]
    error: Optional[StrictStr]

    def to_data(self):
        return {
            name: value for name, value in self.__dict__.items() if value is not None
        }
