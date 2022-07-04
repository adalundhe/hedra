from typing import List
from .command import Command
from .result import Result


class History:

    def __init__(self) -> None:
        self.row_size = 0
        self._history = {}

    def add_row(self, request_name: str, batch_size: int = None):

        if batch_size is None:
            batch_size = self.row_size
        else:
            self.row_size = batch_size

        self._history[request_name] = [
            None for _ in range(self.row_size)
        ]

    def update(self, channel_id: int, response: Command):
        self._history[response.name][channel_id] = response

    def get(self, request_name: str, channel_id: int):
        return self._history.get(request_name)[channel_id]