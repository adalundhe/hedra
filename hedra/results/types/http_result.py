import enum
from hedra.core.engines.types.common.response import Response


class HTTPResult:

    def __init__(self, response: Response) -> None:
        self.name = response.name
        self.url = response.url
        self.ip_addr = response.ip_addr
        self.method = response.method
        self.path = response.path
        self.params = response.params
        self.hostname = response.hostname
        self.checks = response.checks
        self.type = response.type
        self.error = response.error
        self.status = response.status
        self.headers = response.headers
        self.data = response.body
        self.time = response.time