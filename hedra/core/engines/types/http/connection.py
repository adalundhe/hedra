import asyncio
from asyncio import StreamReader, StreamWriter
from ssl import SSLContext
from typing import Optional, Tuple, Union
from hedra.core.engines.types.common.connection_factory import ConnectionFactory


class Connection:

    def __init__(self, reset_connection: bool=False) -> None:
        self.dns_address: str = None
        self.hostname: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.lock = asyncio.Lock()
        self._connection: Tuple[StreamReader, StreamWriter] = ()
        self.connected = False
        self.reset_connection = reset_connection
        self._connection_factory = ConnectionFactory()

    def setup(self, dns_address: str, port: int, ssl: Union[SSLContext, None]) -> None:
        self.dns_address = dns_address
        self.port = port
        self.ssl = ssl

    async def connect(self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]], 
        ssl: Optional[SSLContext]=None
    ) -> Union[Tuple[StreamReader, StreamWriter], Exception]:
        try:
            if self.connected is False or self.dns_address != dns_address or self.reset_connection:
                self._connection = await self._connection_factory.create(hostname, socket_config, ssl=ssl)
                self.connected = True

                self.dns_address = dns_address
                self.port = port
                self.ssl = ssl
                self.hostname = hostname

            return self._connection
        except Exception as e:
            raise e

