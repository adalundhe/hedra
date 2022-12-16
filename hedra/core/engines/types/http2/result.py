import json
from typing import Dict, Union
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from .events.deferred_headers_event import DeferredHeaders
from .action import HTTP2Action


class HTTP2Result(BaseResult):

    __slots__ = (
        'action_id',
        'url',
        'ip_addr',
        'method',
        'path',
        'params',
        'query',
        'hostname',
        'headers',
        'body',
        'response_code',
        'deferred_headers',
        '_compression',
        '_content_type',
        '_size',
        '_version',
        '_reason',
        '_status'
    )

    def __init__(self, action: HTTP2Action, error: Exception = None) -> None:
        super(
            HTTP2Result,
            self
        ).__init__(
            action.action_id,
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
        self.headers: Dict[bytes, bytes] = {}
        self.body = bytearray()
        
        self.response_code: str = None
        self.deferred_headers: DeferredHeaders = None
        self._compression = None
        self._content_type = None
        self._size = None
        self._version = None
        self._reason = None
        self._status = None
        
    def to_dict(self):

        encoded_headers = {
            str(header_name.decode()): str(header_value.decode()) for header_name, header_value in self.headers.items()
        }

        data = self.data
        if isinstance(data, bytes) or isinstance(data, bytearray):
            data = str(data.decode())

        base_result_dict = super().to_dict()

        return {
            'url': self.url,
            'method': self.method,
            'path': self.path,
            'params': self.params,
            'query': self.query,
            'type': self.type,
            'headers': encoded_headers,
            'data': data,
            'tags': self.tags,
            'user': self.user,
            'error': str(self.error),
            'status': self.status,
            'reason': self.reason,
            **base_result_dict
        }

    @property
    def content_type(self):
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            self._content_type = self.headers.get(b'content-type')
        
        return self._content_type

    @content_type.setter
    def content_type(self, value: str):
        self._content_type = value

    @property
    def compression(self):
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            self._compression = self.headers.get(b'content-encoding')

        return self._compression

    @compression.setter
    def compression(self, value: str):
        self._compression = value

    @property
    def version(self) -> Union[str, None]:
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            self._version = self.headers.get(b'version')

        return self._version

    @version.setter
    def version(self, value: str):
        self._version = value

    @property
    def status(self) -> Union[int, None]:
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            self._status = int(self.headers.get(b'status', 400))
        
        return self._status

    @status.setter
    def status(self, value: int):
        self._status = value

    @property
    def reason(self) -> Union[str, None]:
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            self._reason = self.headers.get(b'reason')

        return self._reason

    @reason.setter
    def reason(self, value: str):
        self._reason = value

    @property
    def size(self):
        
        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()
            content_length = self.headers.get(b'content-length')
            if content_length:
                self._size = int(content_length)

            elif len(self.body) > 0:
                self._size = len(self.body)
            
            else:
                self._size = 0

        return self._size

    @size.setter
    def size(self, value: int):
        self._size = value
        
    @property
    def data(self) -> Union[str, dict, None]:

        if len(self.headers) == 0 and self.deferred_headers:
            self.headers = self._parse_headers()

        data = self.body
        try:
            if self.headers.get(b'content-encoding') == b"gzip":
                data = gzip_decompress(self.body)
            elif self.headers.get(b'content-encoding') == b"deflate":
                data = zlib_decompress(self.body)

            if self.headers.get(b'content-type') == b"application/json":
                data = json.loads(self.body)
            
            elif isinstance(self.body, (bytes, bytearray)):
                data = str(self.body.decode())

        except Exception:
            pass

        return data

    def _parse_headers(self):
        try:
            status, decoded_headers = self.deferred_headers.parse()
            decoded_headers['status'] = status
            return decoded_headers

        except Exception:
            return {}
