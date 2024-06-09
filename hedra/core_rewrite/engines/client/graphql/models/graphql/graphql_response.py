import gzip
import re
from typing import Dict, Optional, Type, TypeVar

import orjson
from pydantic import BaseModel, StrictBytes, StrictFloat, StrictInt, StrictStr

from .url_metadata import URLMetadata

space_pattern = re.compile(r"\s+")


T = TypeVar('T', bound=BaseModel)


class GraphQLResponse(BaseModel):
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
    def data(
        self,
        model: Optional[Type[T]]=None
    ):

        content_type = self.headers.get('content-type')

        if model:
            return self.to_model(model)

        try:
            match content_type:

                case 'application/json':
                    return self.json()
                
                case 'text/plain':
                    return self.text()
                
                case 'application/gzip':
                    return gzip.decompress(self.content)
                
                case _:
                    return self.content

        except Exception:
            return self.content
            
 