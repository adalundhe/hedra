import time
from hedra.distributed.discovery.dns.core.url import URL
from typing import List, Iterable, Union
from .exceptions import NoNameServer


class NameServer:

    def __init__(
        self, 
        urls: List[Union[str, URL]]
    ):
        self.data = [
            URL(url) if isinstance(url, str) else url for url in urls
        ]
        

        self._failures = [0] * len(self.data)
        self.timestamp = 0
        self._update()

    def __bool__(self):
        return len(self.data) > 0

    def __iter__(self):
        return iter(self.data)

    def iter(self) -> Iterable[URL]:
        if not self.data: 
            raise NoNameServer()
        
        return iter(self.data)

    def _update(self):

        if time.time() > self.timestamp + 60:
            self.timestamp = time.time()

            self._sorted = list(
                self.data[i] for i in sorted(
                    range(len(self.data)), 
                    key=lambda i: self._failures[i]
                )
            )

            self._failures = [0] * len(self.data)

    def success(self, item):
        self._update()

    def fail(self, item):
        self._update()
        index = self.data.index(item)
        self._failures[index] += 1
