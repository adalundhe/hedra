import binascii
import json
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress
from typing import List, Union
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes


class BaseResponse:

    def __init__(self) -> None:
        self.name = None
        self.url = None
        self.method = None
        self.path = None
        self.hostname = None
        self.checks = None
        self.headers = {}
        self._size = None
        self.content_type = None
        self.compressed = None
        self.body = bytearray()
        self.error = None

        self.time = 0
        self.time_connecting = 0
        self.time_reading = 0
        self.time_writing = 0
        self.time_waiting = 0

        self.user = None
        self.tags = None
        self.extentions = {}
        self.response_code = None
        self.type = None
        self.deferred_headers = None
        self.channel_id = 0

    @property
    def size(self):
        pass
        
    @property
    def data(self) -> Union[str, dict, None]:
        pass

    @property
    def version(self) -> Union[str, None]:
        pass

    @property
    def status(self) -> Union[int, None]:
        pass

    @property
    def reason(self) -> Union[str, None]:
        pass

    def grpc_decode(self, protobuf):
        pass


class Response:

    def __init__(self, 
        request: Request, 
        error: Exception = None, 
        type: str = RequestTypes.HTTP, 
        channel_id: int = 0
    ) -> None:
        self.name = request.name
        self.url = request.url.full
        self.ip_addr = request.url.ip_addr
        self.method = request.method
        self.path = request.url.path
        self.params = request.params.data
        self.hostname = request.url.hostname
        self.checks = request.hooks.checks
        self.type = request.type
        self.headers = {}
        self._size = None
        self.content_type = None
        self.compressed = None
        self.body = bytearray()
        self.error = error

        self.time = 0
        self.start = 0
        self.connect_end = 0
        self.read_end = 0
        self.write_end = 0
        self.wait_start = 0

        self.user = request.metadata.user
        self.tags = request.metadata.tags
        self.extentions = {}
        self.response_code = None
        self.deferred_headers = None
        self.type = type
        self.channel_id = channel_id

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
    def size(self):
        if self._size is None:
            self._size = int(self.headers.get(b"content-length", 0))

        return self._size
        
    @property
    def data(self) -> Union[str, dict, None]:

        self.content_type = self.headers.get(b"content-type")
        self.compressed = self.headers.get(b"content-encoding")
        
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
