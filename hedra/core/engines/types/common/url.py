import aiodns
import socket
from urllib.parse import urlparse
from asyncio.events import get_running_loop
from .types import SocketTypes


class URL:

    def __init__(self, url: str, port: int=80, socket_type: SocketTypes= SocketTypes.DEFAULT) -> None:
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
        self.socket_type = socket_type
        self.loop = None

    async def lookup(self):

        if self.loop is None:
            self.loop = get_running_loop()

        hosts = await self.resolver.query(self.parsed.hostname, 'A')
        resolved = hosts.pop()
        self.ip_addr = resolved.host
        self.has_ip_addr = True

        info = await self.loop.getaddrinfo(
            self.ip_addr, 
            self.port, 
            family=self.socket_type, 
            type=socket.SOCK_STREAM, 
            proto=0, 
            flags=0
        )

        return info

        self.socket_config = info[0]

        return self.ip_addr

    def set_ip_addr_and_port(self, ip_addr):
        self.ip_addr = ip_addr
        
        if self.is_ssl:
            self.port = 443

    @property
    def params(self):
        return self.parsed.params

    @property
    def scheme(self):
        return self.parsed.scheme

    @property
    def hostname(self):
        return self.parsed.hostname

    @property
    def path(self):
        url_path = self.parsed.path

        if len(url_path) == 0:
            url_path = "/"

        if len(self.parsed.query) > 0:
            url_path += f'?{self.parsed.query}'

        return url_path
    @property
    def query(self):
        return self.parsed.query

    @property
    def authority(self):
        return self.parsed.hostname