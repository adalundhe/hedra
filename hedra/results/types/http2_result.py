from hedra.core.engines.types.common.response import Response


class HTTP2Result:

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
        self.time = response.time
        self.status = None
        self.headers = {}
        
        if response.error is None:
            try:
                status, headers = response.deferred_headers.parse()
                self.status = status
                self.headers = headers

            except Exception:
                pass

        self.data = response.body
        self.error = response.error