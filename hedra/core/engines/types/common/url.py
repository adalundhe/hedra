from pydoc import resolve
import aiodns
import socket
from urllib.parse import urlparse
from asyncio.events import get_event_loop
from .types import SocketProtocols, SocketTypes


class URL:

    def __init__(self, url: str, port: int=80, family: SocketTypes=SocketTypes.DEFAULT, protocol: SocketProtocols=SocketProtocols.DEFAULT) -> None:
        self.resolver = aiodns.DNSResolver()
        self.ip_addr = None
        self.parsed = urlparse(url)
        self.is_ssl = 'https' in url or 'wss' in url   

        if self.is_ssl:
            port = 443

        self.port = self.parsed.port if self.parsed.port else port
        self.full = url
        self.has_ip_addr = False 
        self.socket_config = None
        self.family = family
        self.protocol = protocol
        self.loop = None

    async def lookup(self):

        if self.loop is None:
            self.loop = get_event_loop()

        infos = {}
        resolved = await self.resolver.gethostbyname(self.parsed.hostname, self.family)
   
        for address in resolved.addresses:

            info = await self.loop.getaddrinfo(
                address, 
                self.port, 
                family=self.family, 
                type=socket.SOL_SOCKET, 
                proto=0, 
                flags=0
            )

            if infos.get(address) is None:
                infos[address] = [*info]

            else:
                infos[address].extend(info)

        return infos

    @property
    def params(self):
        return self.parsed.params

    @params.setter
    def params(self, value):
        self.parsed.params = value

    @property
    def scheme(self):
        return self.parsed.scheme

    @scheme.setter
    def scheme(self, value):
        self.parsed.scheme = value

    @property
    def hostname(self):
        return self.parsed.hostname

    @hostname.setter
    def hostname(self, value):
        self.parsed.hostname = value


    @property
    def path(self):
        url_path = self.parsed.path

        if len(url_path) == 0:
            url_path = "/"

        if len(self.parsed.query) > 0:
            url_path += f'?{self.parsed.query}'

        elif len(self.parsed.params) > 0:
            url_path += self.parsed.params

        return url_path

    @path.setter
    def path(self, value):
        self.parsed.path = value
        
    @property
    def query(self):
        return self.parsed.query

    @query.setter
    def query(self, value):
        self.parsed.query = value

    @property
    def authority(self):
        return self.parsed.hostname

    @authority.setter
    def authority(self, value):
        self.parsed.hostname = value