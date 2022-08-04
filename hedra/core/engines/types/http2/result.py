import json
from typing import Dict, List, Union
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from .events.deferred_headers_event import DeferredHeaders
from .action import HTTP2Action


class HTTP2Result(BaseResult):

    def __init__(self, action: HTTP2Action, error: Exception = None) -> None:
        super(
            HTTP2Result,
            self
        ).__init__(
            action.name,
            action.url.hostname,
            action.metadata.user,
            action.metadata.tags,
            RequestTypes.HTTP2,
            action.hooks.checks,
            error
        )

        self.url = action.url.full
        self.ip_addr = action.url.ip_addr
        self.method = action.method
        self.path = action.url.path
        self.params = action.url.params
        self.query = action.url.query
        self.hostname = action.url.hostname
        self._headers: Dict[bytes, bytes] = None
        self.body = bytearray()
        
        self.response_code: str = None
        self.deferred_headers: DeferredHeaders = None

    def to_dict(self):

        encoded_headers = {
            header_name.decode(): header_value.decode() for header_name, header_value in self.headers.items()
        }

        data = self.data
        if isinstance(data, bytes) or isinstance(data, bytearray):
            data = data.decode()

        return {
            'name': self.name,
            'url': self.url,
            'method': self.method,
            'path': self.path,
            'params': self.params,
            'type': self.type,
            'headers': encoded_headers,
            'data': data,
            'tags': self.tags,
            'user': self.user,
            'error': str(self.error),
            'status': self.status,
            'reason': self.reason
        }

    @property
    def content_type(self):
        if self._headers is None:
            return self.headers.get(b'content-type')

        return self._headers.get(b'content-type')

    @property
    def compression(self):
        if self._headers is None:
            return self.headers.get(b'content-encoding')

        return self._headers.get(b'content-encoding')

    @property
    def headers(self):
        if self._headers is None:
            self._headers = self.deferred_headers.parse()
            

        return self._headers

    @property
    def size(self):
        if self._headers is None:
            return int(self.headers.get(b'content-length', 0))

        return int(self._headers.get(b"content-length", 0))
        
    @property
    def data(self) -> Union[str, dict, None]:

        data = self.body
        try:
            if self.compressed == b"gzip":
                data = gzip_decompress(self.body)
            elif self.compressed == b"deflate":
                data = zlib_decompress(self.body)

            if self.content_type == b"application/json":
                data = json.loads(self.body)
            
            elif isinstance(self.body, bytes):
                data = data.decode()

        except Exception:
            pass

        return data

    @property
    def version(self) -> Union[str, None]:
        try:
            status_string: List[bytes] = self.response_code.split()
            return status_string[0].decode()
        except Exception:
            return None

    @property
    def status(self) -> Union[int, None]:
        try: 
            if isinstance(self.response_code, bytes) or isinstance(self.response_code, str):
                status_string: List[bytes] = self.response_code.split()
                return int(status_string[1])
            else:
                return self.response_code

        except Exception as e:
            return None

    @property
    def reason(self) -> Union[str, None]:
        try:
            if isinstance(self.response_code, bytes) or isinstance(self.response_code, str):
                status_string: List[bytes] = self.response_code.split()
                return status_string[2].decode()
            
            return None

        except Exception:
            return None