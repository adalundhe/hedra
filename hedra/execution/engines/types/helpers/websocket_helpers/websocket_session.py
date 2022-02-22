class WebsocketSession:

    def __init__(self, config) -> None:
        self.url = config.get('url')
        self.headers = config.get('headers')
        self.method = config.get('method')
        self.auth = config.get('auth')