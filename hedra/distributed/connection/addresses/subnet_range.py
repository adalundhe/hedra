import ipaddress
from typing import List


class SubnetRange:

    def __init__(
        self,
        base_address: str,
        subnet_range: int=24
    ) -> None:
        self.subnet = f'{base_address}/{subnet_range}'
        self._network = ipaddress.ip_network(self.subnet, strict=False)
        self._addresses = [
            str(ip) for ip in self._network.hosts()
        ]

        self.reserved: List[str] = []

    def __iter__(self):

        available_addresses = [
            address for address in self._addresses if address not in self.reserved
        ]

        for address in available_addresses:
                yield address