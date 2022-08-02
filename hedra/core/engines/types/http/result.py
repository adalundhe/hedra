import json
from gzip import decompress as gzip_decompress
from typing import List, Union
from zlib import decompress as zlib_decompress
from hedra.core.engines.types.common.types import RequestTypes
from .action import HTTPAction

class HTTPResult:

    def __init__(self, action: HTTPAction, error: Exception=None) -> None:
        self.name = action.name
        self.url = action.url.full
        self.ip_addr = action.url.ip_addr
        self.method = action.method
        self.path = action.url.path
        self.params = action._params
        self.hostname = action.url.hostname
        self.checks = action.hooks.checks
        self.type = RequestTypes.HTTP

        self.headers = {}
        self.body = bytearray()
        self.error = error

        self.time = 0
        self.start = 0
        self.connect_end = 0
        self.read_end = 0
        self.write_end = 0
        self.wait_start = 0

        self.user = action.metadata.user
        self.tags = action.metadata.tags
        self.extentions = {}
        self.response_code = None
        self.deferred_headers = None

    @property
    def content_type(self):
        return self.headers.get('content-type')

    @property
    def compression(self):
        return self.headers.get(b"content-encoding")

    @property
    def size(self):
        if self.headers.get('content-length'):
            return int(self.headers.get('content-length'))
        
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