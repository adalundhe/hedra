import binascii
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Tuple,
)
from urllib.parse import urlparse

from pydantic import BaseModel, StrictInt, StrictStr

from .url import URL

NEW_LINE = '\r\n'

class GraphQLHTTP2Request(BaseModel):
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
    headers: Dict[str, str]={}
    data: Dict[
        Literal[
            'query', 
            'operation_name', 
            'variables'
        ], 
        str
    ]={}
    protobuf: Any=None
    redirects: StrictInt=3

    class Config:
        arbitrary_types_allowed=True

    def parse_url(self):
        return urlparse(self.url)

    def encode_data(self):

        encoded_protobuf = str(binascii.b2a_hex(self.protobuf.SerializeToString()), encoding='raw_unicode_escape')
        encoded_message_length = hex(int(len(encoded_protobuf)/2)).lstrip("0x").zfill(8)
        encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'

        return binascii.a2b_hex(encoded_protobuf)

    def encode_headers(
        self,
        url: URL,
        timeout: int | float=60
    ) -> List[Tuple[bytes, bytes]]:
    
        encoded_headers = [
            (b":method", self.method.encode()),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url.path.encode()),
            (b'Content-Type', b'application/grpc'),
            (b'Grpc-Timeout', f'{timeout}'.encode()),
            (b'TE', b'trailers')
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