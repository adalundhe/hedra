from __future__ import annotations
from typing import Union, Dict
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.result import HTTPResult
from .action import HTTP3Action

try:
    from aioquic.h3.events import (
        HeadersReceived,
        DataReceived
    )

except ImportError:
    HeadersReceived = object
    DataReceived = object
    

class HTTP3Result(HTTPResult):

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
        'headers_frame',
        'body',
        'response_code',
        '_version',
        '_reason',
        '_status'
    )

    def __init__(
            self, 
            action: HTTP3Action, 
            error: Exception = None
        ) -> None:
        super().__init__(
            action, 
            error
        )

        self.body = bytearray()
        self.headers_frame: HeadersReceived = None
        self.body: DataReceived = None
        self.type = RequestTypes.HTTP3

    @property
    def status(self) -> Union[int, None]:
        try:
            response_status = self.headers.get(b':status')
            if self._status is None and isinstance(response_status, (bytes, bytearray)):
                self._status = int(response_status)

        except Exception:
            pass

        return self._status
    
    @classmethod
    def from_dict(
        cls, 
        results_dict: Dict[str, Union[int, float, str,]]
    ) -> HTTP3Result:
        
        action = HTTP3Action(
            results_dict.get('name'),
            results_dict.get('url'),
            method=results_dict.get('method'),
            user=results_dict.get('user'),
            tags=results_dict.get('tags'),
        )

        response = HTTP3Result(action, error=results_dict.get('error'))
        

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
        