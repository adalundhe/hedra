from .http_event import HttpEvent


class WebsocketEvent(HttpEvent):

    def __init__(self, action):
        super(WebsocketEvent, self).__init__(action)