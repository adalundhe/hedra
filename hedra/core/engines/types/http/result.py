import json
from gzip import decompress as gzip_decompress
from typing import List, Union, Dict
from zlib import decompress as zlib_decompress
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from .action import HTTPAction

class HTTPResult(BaseResult):

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
        '_version',
        '_reason',
        '_status'
    )

    def __init__(self, action: HTTPAction, error: Exception=None) -> None:

        super(
            HTTPResult,
            self
        ).__init__(
            action.action_id,
            action.name,
            action.url.hostname,
            action.metadata.user,
            action.metadata.tags,
            RequestTypes.HTTP,
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
        self._version = None
        self._reason = None
        self._status = None

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
            
            elif isinstance(self.body, (bytes, bytearray)):
                data = str(data.decode())

        except Exception:
            pass

        return data

    @data.setter
    def data(self, value):
        self.body = value

    @property
    def version(self) -> Union[str, None]:
        try:
            if self._version is None and isinstance(self.response_code, (bytes, bytearray)):
                status_string: List[bytes] = self.response_code.split()
                self._version = status_string[0].decode()
        except Exception:
            pass
            
        return self._version

    @version.setter
    def version(self, new_version: str):
        self._version = new_version

    @property
    def status(self) -> Union[int, None]:
        try:

            if self._status is None and isinstance(self.response_code, (bytes, bytearray)):
                status_string: List[bytes] = self.response_code.split()
                self._status = int(status_string[1])

        except Exception:
            pass

        return self._status

    @status.setter
    def status(self, new_status: int):
        self._status = new_status

    @property
    def reason(self) -> Union[str, None]:

        try:

            if self._reason is None and isinstance(self.response_code, (bytes, bytearray)):
                status_string: List[bytes] = self.response_code.split()
                self._reason = status_string[2].decode()

        except Exception:
            pass
        
        return self._reason

    @reason.setter
    def reason(self, new_reason):
        self._reason = new_reason

    def to_dict(self):

        encoded_headers = {
            str(k.decode()): str(v.decode()) for k, v in self.headers.items()
        }

        base_result_dict = super().to_dict()

        data = self.data
        if isinstance(data, bytes) or isinstance(data, bytearray):
            data = str(data.decode())
        
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

    @classmethod
    def from_dict(cls, results_dict: Dict[str, Union[int, float, str,]]):

        action = HTTPAction(
            results_dict.get('name'),
            results_dict.get('url'),
            method=results_dict.get('method'),
            user=results_dict.get('user'),
            tags=results_dict.get('tags'),
        )

        response = HTTPResult(action, error=results_dict.get('error'))
        

        response.headers.update(results_dict.get('headers', {}))
        response.data = results_dict.get('data')
        response.status = results_dict.get('status')
        response.reason = results_dict.get('reason')
        response.checks = results_dict.get('checks')
     
        response.wait_start = results_dict.get('wait_start')
        response.start = results_dict.get('start')
        response.connect_end = results_dict.get('connect_end')
        response.write_end = results_dict.get('write_end')
        response.complete = results_dict.get('complete')

        return response