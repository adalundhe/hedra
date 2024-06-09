from typing import Dict, Iterator, List, Literal, Optional, Tuple, Union
from urllib.parse import urlencode, urlparse

import orjson
from pydantic import BaseModel, StrictBytes, StrictInt, StrictStr

from .types import HTTPCookie, HTTPEncodableValue
from .url import URL

NEW_LINE = '\r\n'

class HTTP2Request(BaseModel):
    url: StrictStr
    method: Literal[
        "GET", 
        "POST",
        "HEAD",
        "OPTIONS", 
        "PUT", 
        "PATCH", 
        "DELETE"
    ]
    cookies: Optional[List[HTTPCookie]]=None
    auth: Optional[Tuple[str, str]]=None
    params: Optional[Dict[str, HTTPEncodableValue]]=None
    headers: Dict[str, str]={}
    data: Union[
        Optional[StrictStr],
        Optional[StrictBytes],
        Optional[BaseModel]
    ]=None
    redirects: StrictInt=3

    class Config:
        arbitrary_types_allowed=True

    def parse_url(self):
        return urlparse(self.url)

    def encode_data(self):

        encoded_data: Optional[bytes] = None
        size = 0
        if self.data:
            if isinstance(self.data, Iterator):
                chunks = []
                for chunk in self.data:
                    chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                    encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                    size += len(encoded_chunk)
                    chunks.append(encoded_chunk)

                self.is_stream = True
                encoded_data = chunks

            else:

                if isinstance(self.data, dict):
                    encoded_data = orjson.dumps(
                        self.data
                    )

                elif isinstance(self.data, BaseModel):
                    return self.data.model_dump_json().encode()

                elif isinstance(self.data, tuple):
                    encoded_data = urlencode(
                        self.data
                    ).encode()

                elif isinstance(self.data, str):
                    encoded_data = self.data.encode()

        return encoded_data

    def encode_headers(
        self,
        url: URL
    ) -> List[Tuple[bytes, bytes]]:
    
        encoded_headers = [
            (b":method", self.method.encode()),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url.path.encode()),
        ]

        encoded_headers.extend([
            (
                k.lower().encode(), 
                v.encode()
            )
            for k, v in self.headers.items()
            if k.lower()
            not in (
                b"host",
                b"transfer-encoding",
            )
        ])
        
        return encoded_headers