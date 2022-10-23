import json
from typing import Dict
from hedra.core.engines.types.http2 import HTTP2Result
from .base_event import BaseEvent


class HTTP2Event(BaseEvent):

    __slots__ = (
        'url',
        'ip_addr',
        'method',
        'path',
        'params',
        'hostname',
        'status',
        'headers',
        'data',
        'status'
    )

    def __init__(self, result: HTTP2Result) -> None:
        super(HTTP2Event, self).__init__(result)

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

    def serialize(self):

        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        serializable_headers = {}
        for key, value in self.headers.items():
            serializable_headers[key.decode()] = value.decode()
            
        return json.dumps({
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
            'data': data
        })
