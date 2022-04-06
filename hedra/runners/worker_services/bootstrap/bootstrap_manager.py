import asyncio
from .bootstrap_client import BoostrapClient


class BootstrapManager:

    def __init__(self, config) -> None:
        self.client = BoostrapClient(config)
        self.use_bootstrap = self.client.use_bootstrap

    async def discovered(self):
        return list(self.client.registry.discovered.values())
