import binascii
import json
from gzip import decompress as gzip_decompress
from urllib.parse import ParseResult
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
        self.body = b''
        self.error = None
        self.time = 0
        self.user = None
        self.tags = None
        self.extentions = {}
        self.response_code = None
        self.type = None
        self.deferred_headers = None
        self.channel_id = 0

    def _set_response_headers(self, response_headers: dict = {}):
        pass

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
        self.checks = request.checks
        self.type = request.type
        self.headers = {}
        self._size = None
        self.content_type = None
        self.compressed = None
        self.body = b''
        self.error = error
        self.time = 0
        self.user = request.metadata.user
        self.tags = request.metadata.tags
        self.extentions = {}
        self.response_code = None
        self.deferred_headers = None
        self.type = type
        self.channel_id = channel_id

    def _set_response_headers(self, response_headers: dict = {}):
        self.content_type = response_headers.get("content-type", "")
        self.compressed = response_headers.get("content-encoding", "")

    @property
    def size(self):
        if self._size is None:
            self._size = int(self.headers.get("content-length", 0))

        return self._size
        
    @property
    def data(self) -> Union[str, dict, None]:
        data = self.body
        if self.compressed == "gzip":
            data = gzip_decompress(self.body)
        elif self.compressed == "deflate":
            data = zlib_decompress(self.body)

        if self.content_type == "application/json":
            data = json.loads(self.body)
        
        elif isinstance(self.body, bytes):
            data = data.decode()

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

    def grpc_decode(self, protobuf):
        wire_msg = binascii.b2a_hex(self.body)

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]
        protobuf.ParseFromString(binascii.a2b_hex(msg))

        return protobuf
