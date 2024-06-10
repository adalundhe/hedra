import gzip
import re
from typing import Dict, Literal, Optional, Type, TypeVar, Union

import orjson
from pydantic import BaseModel, StrictBytes, StrictFloat, StrictInt, StrictStr

from hedra.core_rewrite.engines.client.shared.models import (
    Cookies,
    URLMetadata,
)

space_pattern = re.compile(r"\s+")


T = TypeVar('T', bound=BaseModel)


class HTTPResponse(BaseModel):
    url: URLMetadata
    method: Optional[
        Literal[
            "GET", 
            "POST",
            "HEAD",
            "OPTIONS", 
            "PUT", 
            "PATCH", 
            "DELETE"
        ]
    ]=None
    cookies: Union[
        Optional[Cookies],
        Optional[None]
    ]=None
    status: Optional[StrictInt]=None
    status_message: Optional[StrictStr]=None
    headers: Dict[StrictBytes, StrictBytes]={}
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
            
 