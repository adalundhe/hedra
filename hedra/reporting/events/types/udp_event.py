import json
from hedra.core.engines.types.udp import UDPResult
from .base_event import BaseEvent


class UDPEvent(BaseEvent):

    __slots__ = (
        'url',
        'ip_addr',
        'path',
        'method',
        'headers',
        'params',
        'hostname',
        'status',
        'data'
    )

    def __init__(self, result: UDPResult) -> None:
        super(UDPEvent, self).__init__(result)

        self.url = result.url
        self.ip_addr = result.ip_addr
        self.path = result.path
        self.method = 'n/a'
        self.headers = {}
        self.params = result.params
        self.hostname = result.hostname
        self.status = result.status
        self.data = result.data
        self.name = self.shortname

    def serialize(self):

        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

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
            'headers': self.headers,
            'data': data
        })
