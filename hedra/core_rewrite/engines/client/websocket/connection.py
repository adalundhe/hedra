from hedra.core_rewrite.engines.client.http.protocols import HTTPConnection


class WebsocketConnection(HTTPConnection):
    def __init__(self, reset_connections: bool = False) -> None:
        super().__init__(reset_connections=reset_connections)
