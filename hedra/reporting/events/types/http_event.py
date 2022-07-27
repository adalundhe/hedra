import json
from hedra.core.engines.types.common.response import Response
from .base_event import BaseEvent


class HTTPEvent(BaseEvent):

    def __init__(self, response: Response) -> None:
        super(HTTPEvent, self).__init__(response)

        self.url = response.url
        self.ip_addr = response.ip_addr
        self.method = response.method
        self.path = response.path
        self.params = response.params
        self.hostname = response.hostname
        self.status = response.status
        self.headers = response.headers
        self.data = response.data
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
