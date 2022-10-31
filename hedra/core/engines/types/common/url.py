from re import S
import traceback
from typing import Dict
import aiodns
import socket
from ipaddress import ip_address, IPv4Address
from urllib.parse import urlparse
from asyncio.events import get_event_loop
from .types import SocketProtocols, SocketTypes


class URL:

    __slots__ = (
        'resolver',
        'ip_addr',
        'parsed',
        'is_ssl',
        'port',
        'full',
        'has_ip_addr',
        'socket_config',
        'family',
        'protocol',
        'loop'
    )

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

        if self.parsed.hostname is None:
            
            try:

                address = self.full.split(':')
                assert len(address) == 2

                host, port = address
                self.port = int(port)

                if isinstance(ip_address(host), IPv4Address):
                    socket_type = socket.AF_INET

                else:
                    socket_type = socket.AF_INET6
                
                infos[self.full] = [(
                    socket_type,
                    self.protocol,
                    None,
                    None,
                    (
                        host,
                        self.port
                    )
                )]
            
            except Exception as parse_error:
                raise parse_error

        else:

            resolved = await self.resolver.gethostbyname(self.parsed.hostname, self.family)
    
            for address in resolved.addresses:

                if isinstance(ip_address(address), IPv4Address):
                    socket_type = socket.AF_INET

                else:
                    socket_type = socket.AF_INET6


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
    def params(self, value: str):
        self.parsed = self.parsed._replace(params=value)

    @property
    def scheme(self):
        return self.parsed.scheme

    @scheme.setter
    def scheme(self, value):
        self.parsed = self.parsed._replace(scheme=value)

    @property
    def hostname(self):
        return self.parsed.hostname

    @hostname.setter
    def hostname(self, value):
        self.parsed = self.parsed._replace(hostname=value)

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
        self.parsed = self.parsed._replace(path=value)
        
    @property
    def query(self):
        return self.parsed.query

    @query.setter
    def query(self, value):
        self.parsed = self.parsed._replace(query=value)

    @property
    def authority(self):
        return self.parsed.hostname

    @authority.setter
    def authority(self, value):
        self.parsed = self.parsed._replace(hostname=value)