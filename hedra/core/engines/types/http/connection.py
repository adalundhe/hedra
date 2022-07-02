import asyncio
from asyncio import StreamReader, StreamWriter
from ssl import SSLContext
from typing import Optional, Tuple, Union


class Connection:

    def __init__(self, reset_connection: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.request_name = None
        self.lock = asyncio.Lock()
        self._connection: Tuple[StreamReader, StreamWriter] = ()
        self.connected = False
        self.reset_connection = reset_connection

    def setup(self, dns_address: str, port: int, ssl: Union[SSLContext, None]) -> None:
        self.dns_address = dns_address
        self.port = port
        self.ssl = ssl

    async def connect(self, request_name: str, dns_address: str, port: int, ssl: Optional[SSLContext]=None) -> Union[Tuple[StreamReader, StreamWriter], Exception]:
        try:
            if self.connected is False or self.request_name != request_name or self.reset_connection:
                self._connection = await asyncio.open_connection(dns_address, port, ssl=ssl)
                self.connected = True

                self.dns_address = dns_address
                self.port = port
                self.ssl = ssl
                self.request_name = request_name

            return self._connection
        except Exception as e:
            raise e

