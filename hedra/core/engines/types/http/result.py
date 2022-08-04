import json
from gzip import decompress as gzip_decompress
from typing import List, Union
from zlib import decompress as zlib_decompress
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from .action import HTTPAction

class HTTPResult(BaseResult):

    def __init__(self, action: HTTPAction, error: Exception=None) -> None:

        super(
            HTTPResult,
            self
        ).__init__(
            action.name,
            action.url.hostname,
            action.metadata.user,
            action.metadata.tags,
            RequestTypes.HTTP,
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

        self.headers = {}
        self.body = bytearray()
        self.response_code = None

    @property
    def content_type(self):
        return self.headers.get(b'content-type')

    @property
    def compression(self):
        return self.headers.get(b"content-encoding")

    @property
    def size(self):
        if self.headers.get(b'content-length'):
            return int(self.headers.get(b'content-length'))
        
        elif self.body:
            return len(self.body)
        
        else:
            return 0

    @property
    def data(self) -> Union[str, dict, None]:
        data = self.body
        try:
            if self.compression == b"gzip":
                data = gzip_decompress(self.body)
            elif self.compression == b"deflate":
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