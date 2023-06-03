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
 
        