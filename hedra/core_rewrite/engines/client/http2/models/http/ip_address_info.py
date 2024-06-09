import socket
from typing import (
    Literal,
    Optional,
    Tuple,
    Union 
)


class IpAddressInfo:
    
    def __init__(
        self,
        family: Union[
            Literal[socket.AF_INET],
            Literal[socket.AF_INET6]
        ],
        socket_type: Literal[socket.SOCK_STREAM],
        protocol: Optional[int],
        cannonical_name: Optional[str],
        address: Union[
            Tuple[
                str,
                int
            ],
            Tuple[
                str,
                int,
                int,
                int
            ]
        ]

    ) -> None:
        
        if family == socket.AF_INET:
            host, port = address

        else:
            host, port, _, _ = address

        self.family = family
        self.socket_type = socket_type
        self.protocol = protocol
        self.cannonical_name = cannonical_name
        self.host = host
        self.port = port
        self.address = address

        self.is_ipv6 = family == socket.AF_INET6