import aiodns
import socket
from ipaddress import ip_address, IPv4Address
from urllib.parse import urlparse
from asyncio.events import get_event_loop
from typing import List, Union, Tuple
from .types import SocketProtocols, SocketTypes
from .ip_address_info import IpAddressInfo


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
        'loop',
        'ip_addresses',
        'address'
    )

    def __init__(
        self,
        url: str, 
        port: int=80,
        family: SocketTypes=SocketTypes.DEFAULT, 
        protocol: SocketProtocols=SocketProtocols.DEFAULT
    ) -> None:
        
        self.resolver = aiodns.DNSResolver()
        self.parsed = urlparse(url)
        self.is_ssl = 'https' in url or 'wss' in url   

        if self.is_ssl:
            port = 443

        self.port = self.parsed.port if self.parsed.port else port
        self.full = url
        self.has_ip_addr = False 
        self.family = family
        self.protocol = protocol
        self.loop = None
        self.ip_addresses: List[Tuple[str, IpAddressInfo]] = []
        self.address: Union[str, None] = None
        self.socket_config: Union[
            Tuple[str, int],
            Tuple[str, int, int, int],
            None
        ] = None

    async def replace(self, url: str):
        self.full = url
        self.params = urlparse(url)

    def __iter__(self):
        for ip_info in self.ip_addresses:
            yield ip_info
    
    def update(
        self,
        url: str,
        port: int = 80,
        family: SocketTypes=SocketTypes.DEFAULT, 
        protocol: SocketProtocols=SocketProtocols.DEFAULT
    ):
        self.parsed = urlparse(url)
        self.is_ssl = 'https' in url or 'wss' in url   

        if self.is_ssl:
            port = 443

        self.port = self.parsed.port if self.parsed.port else port
        self.full = url
        self.has_ip_addr = False 
        self.socket_config: Union[
            Tuple[str, int],
            Tuple[str, int, int, int],
            None
        ] = None
        self.family = family
        self.protocol = protocol
        self.loop = None
        self.ip_addresses: List[IpAddressInfo] = []
        self.address: Union[str, None] = None

    async def lookup(self):

        if self.loop is None:
            self.loop = get_event_loop()

        if self.parsed.hostname is None:
            
            try:

                address = self.full.split(':')
                assert len(address) == 2

                host, port = address
                self.port = int(port)

                if isinstance(ip_address(host), IPv4Address):
                    socket_family = socket.AF_INET

                else:
                    socket_family = socket.AF_INET6
                
                self.ip_addresses = [
                    IpAddressInfo(
                        socket_family,
                        self.protocol,
                        None,
                        None,
                        (
                            host,
                            self.port
                        )
                    )
                ]
            
            except Exception as parse_error:
                raise parse_error

        else:

            resolved = await self.resolver.gethostbyname(self.parsed.hostname, self.family)
    
            for address in resolved.addresses:

                if isinstance(ip_address(address), IPv4Address):
                    socket_family = socket.AF_INET
                    address_info = (
                        address,
                        self.port
                    )

                else:
                    socket_family = socket.AF_INET6
                    address_info = (
                        address,
                        self.port,
                        0,
                        0
                    )

                self.ip_addresses.append((
                    address,
                    (
                        socket_family,
                        socket.SOCK_STREAM,
                        0,
                        '',
                        address_info
                    )
                ))

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
        url_query = self.parsed.query
        url_params = self.parsed.params

        if not url_path or len(url_path) == 0:
            url_path = "/"

        if url_query and len(url_query) > 0:
            url_path += f'?{self.parsed.query}'

        elif url_params and len(url_params) > 0:
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