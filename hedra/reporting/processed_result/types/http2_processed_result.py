import json
from typing import Dict, Any, Union
from hedra.core.engines.types.http2 import HTTP2Result
from .base_processed_result import BaseProcessedResult


class HTTP2ProcessedResult(BaseProcessedResult):

    __slots__ = (
        'event_id',
        'action_id',
        'url',
        'ip_addr',
        'method',
        'path',
        'params',
        'hostname',
        'status',
        'headers',
        'data',
        'status',
        'timings'
    )

    def __init__(
        self, 
        stage: str, 
        result: HTTP2Result
    ) -> None:
        super(HTTP2ProcessedResult, self).__init__(
            stage,
            result
        )

        self.url = result.url
        self.ip_addr = result.ip_addr
        self.method = result.method
        self.path = result.path
        self.params = result.params
        self.hostname = result.hostname
        self.status = None
        self.headers: Dict[bytes, bytes] = result.headers
        self.data = result.data
        self.status = result.status
        
        self.name = f'{self.method}_{self.shortname}'

        self.time = result.complete - result.start

        self.timings = {
            'total': self.time,
            'waiting': result.start - result.wait_start,
            'connecting': result.connect_end - result.start,
            'writing': result.write_end - result.connect_end,
            'reading': result.complete - result.write_end
        }

    def to_dict(self) -> Dict[str, Union[str, int, float]]:

        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        serializable_headers = {}
        for key, value in self.headers.items():
            serializable_headers[key.decode()] = value.decode()
            
        return {
            'name': self.name,
            'stage': self.stage,
            'shortname': self.shortname,
            'checks': [check.__name__ for check in self.checks],
            'error': str(self.error),
            'time': self.time,
            'type': self.type,
            'source': self.source,
            'url': self.url,
            'ip_addr': self.ip_addr,
            'method': self.method,
            'path': self.path,
            'params': self.params,
            'hostname': self.hostname,
            'status': self.status,
            'headers': serializable_headers,
            'data': data,
            **self.timings
        }

    def serialize(self) -> str:

        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        serializable_headers = {}
        for key, value in self.headers.items():
            serializable_headers[key.decode()] = value.decode()
            
        return json.dumps(
            self.to_dict()
        )
