from hedra.core.engines.types.http.connection import HTTPConnection


class WebsocketConnection(HTTPConnection):


    def __init__(self, reset_connection: bool = False) -> None:
        super().__init__(reset_connection)

    def iter_headers(self):
        return self.reader.iter_headers()