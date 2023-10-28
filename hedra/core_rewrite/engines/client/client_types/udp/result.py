from __future__ import annotations
from typing import Union, Dict
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from .action import UDPAction


class UDPResult(BaseResult):

    __slots__ = (
        'action_id',
        'url',
        'ip_addr',
        'has_response',
        'path',
        'params',
        'query',
        'hostname',
        'body',
        'response_code',
        '_version',
        '_reason'
        '_status'
    )

    def __init__(self, action: UDPAction, error: Exception=None) -> None:

        super(
            UDPResult,
            self
        ).__init__(
            action.action_id,
            action.name,
            action.url.hostname,
            action.metadata.user,
            action.metadata.tags,
            RequestTypes.UDP,
            error
        )

        self.url = action.url.full
        self.ip_addr = action.url.ip_addr
        self.has_response = action.wait_for_response
        self.path = action.url.path
        self.params = action.url.params
        self.query = action.url.query
        self.hostname = action.url.hostname

        self.body = bytearray()
        self.response_code = None
        self._version = None
        self._reason = None
        self._status = None

    @property
    def size(self):
        
        if self.body:
            return len(self.body)
        
        else:
            return 0

    @property
    def data(self) -> Union[str, dict, None]:
        return self.body.decode()

    @property
    def status(self) -> Union[int, None]:
        return self._status

    @status.setter
    def status(self, new_status: int):
        self._status = new_status
