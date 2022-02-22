import asyncio
from .bootstrap_client import BoostrapClient


class BootstrapManager:

    def __init__(self, config) -> None:
        self.client = BoostrapClient(config)

        self.use_discovery = (self.client.bootsrap_ip and self.client.bootstrap_port) or self.client.bootstrap_service_address

    async def discovered(self):
        return list(self.client.registry.discovered.values())
