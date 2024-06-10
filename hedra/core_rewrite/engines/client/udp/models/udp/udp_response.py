from typing import Dict, Optional, Type, TypeVar

import orjson
from pydantic import BaseModel, StrictBytes, StrictFloat, StrictStr

from hedra.core_rewrite.engines.client.shared.models import (
    URLMetadata,
)

T = TypeVar('T', bound=BaseModel)


class UDPResponse(BaseModel):
    url: URLMetadata
    error: Optional[StrictStr]=None
    content: StrictBytes=b''
    timings: Dict[StrictStr, StrictFloat]={}

    class Config:
        arbitrary_types_allowed=True
    
    def json(self):

        if self.content:
            return orjson.loads(
                self.content
            )
    
        return {}
        
    def text(self):
        return self.content.decode()
    
    def to_model(
        self,
        model: Type[T]
    ) -> T:
        return model(**orjson.loads(
            self.content
        ))

    @property
    def data(self):
        return self.content
 