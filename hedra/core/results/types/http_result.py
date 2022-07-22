from hedra.core.engines.types.common.response import Response
from .base_result import BaseResult


class HTTPResult(BaseResult):

    def __init__(self, response: Response) -> None:
        super(HTTPResult, self).__init__(response)

        self.url = response.url
        self.ip_addr = response.ip_addr
        self.method = response.method
        self.path = response.path
        self.params = response.params
        self.hostname = response.hostname
        self.status = response.status
        self.headers = response.headers
        self.data = response.data

    