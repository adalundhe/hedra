from easy_logger import Logger
from .http_response import HttpResponse


class WebsocketResponse(HttpResponse):

    def __init__(self, action):
        super(WebsocketResponse, self).__init__(action)