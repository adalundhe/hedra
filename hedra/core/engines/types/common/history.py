from typing import List
from .request import Request
from .response import Response


class History:

    def __init__(self) -> None:
        self.row_size = 0
        self._history = {}

    def add_row(self, request_name: str):
        self._history[request_name] = []

    def update(self, channel_id: int, response: Response):
        self._history[response.name][channel_id] = response

    def get(self, request_name: str, channel_id: int):
        return self._history.get(request_name)[channel_id]