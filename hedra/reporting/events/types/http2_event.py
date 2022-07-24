from hedra.core.engines.types.common.response import Response
from .base_event import BaseEvent


class HTTP2Event(BaseEvent):

    def __init__(self, response: Response) -> None:
        super(HTTP2Event, self).__init__(response)

        self.url = response.url
        self.ip_addr = response.ip_addr
        self.method = response.method
        self.path = response.path
        self.params = response.params
        self.hostname = response.hostname
        self.status = None
        self.headers = {}
        self.data = response.body
        
        if response.error is None:
            try:
                status, headers = response.deferred_headers.parse()
                self.status = status
                self.headers = headers

            except Exception:
                pass

        
        self.name = f'{self.method}_{self.shortname}'
