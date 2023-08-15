from __future__ import annotations
import re
import socket
from hedra.distributed.discovery.dns.core.record import RecordType
from typing import Tuple, Union, Optional
from urllib.parse import urlparse
from .exceptions import (
    InvalidHost,
    InvalidIP
)


ip_pattern = '(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
match_pattern = re.compile(ip_pattern)



class URL:



    def __init__(
        self, 
        url: str,
        port: Optional[int]=None
    ):

        self._default_ports = {
            'tcp': 53,
            'udp': 53,
            'tcps': 853,
            'http': 80,
            'https': 443,
        }
        

        self.url = url
        self.parsed = urlparse(url)

        self.host = self.parsed.hostname

        if port is None:
            port = self.parsed.port

        self.port = port

        if self.host is None:
            (
                _,
                host,
                _
            ) = self.parse_netloc()

            self.host = host

        self.is_ssl = False
        if self.parsed.scheme in ['tcps', 'https', 'msyncs']:
            self.is_ssl = True

        self.ip_type = self.get_ip_type(
            self.host
        )

        if self.ip_type is None:

            matches = re.search(
                ip_pattern,
                self.url
            )
            self.host = matches.group('host')
            self.port = matches.group('port')

            if self.port:
                self.port = int(self.port)
        

        if self.port is None or self.port == '':
            self.port = self._default_ports.get(
                self.parsed.scheme,
                80
            )

        self.domain_protocol_map = {
            "tcp": "tcp",
            "udp": "udp",
            "tcps": "tcp",
            "http": "tcp",
            "https": "tcp"
        }

        self.address = (
            self.host, 
            self.port
        )

        self.is_msync = self.parsed.scheme in ['msync', 'msyncs']

    def __str__(self):
        return self.url

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    def copy(self):
        return URL(self.url)
    
    def parse_netloc(self):

        authentication: Union[str, None] = None
        port: Union[str, None] = None

        host = self.parsed.netloc

        if '@' in host:
            authentication, host = host.split('@')

        if ':' in host:
            host, port = host.split(':')

        if port:
            port = int(port)

        return (
            authentication,
            host,
            port
        )
            

    def to_ptr(self):
        if self.ip_type is RecordType.A:
            reversed_hostname =  '.'.join(
                self.parsed.hostname.split('.')[::-1]
            )

            return f'{reversed_hostname}.in-addr.arpa'
        
        raise InvalidIP(self.parsed.hostname)
    
    def get_ip_type(
        self,
        hostname: str
    ):

        if ':' in hostname:
            # ipv6
            try:
                socket.inet_pton(socket.AF_INET6, hostname)
            except OSError:
                raise InvalidHost(hostname)
            
            return RecordType.AAAA
        
        try:
            socket.inet_pton(socket.AF_INET, hostname)
        except OSError:
            # domain name
            pass
        else:
            return RecordType.A

    
    @property
    def domain_protocol(self):
        return self.domain_protocol_map.get(self.parsed.scheme, "udp")
