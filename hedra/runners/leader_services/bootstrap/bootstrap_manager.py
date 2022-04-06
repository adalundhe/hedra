import asyncio
from .bootstrap_client import BoostrapClient


class BootstrapManager:

    def __init__(self, config) -> None:
        self.client = BoostrapClient(config)
        self.use_bootstrap = self.client.use_bootstrap

    async def register_leader(self, leader_address):
        await self.client.register_self(leader_address)

    async def wait_for_leaders(self, leader_address, leader_id):
        while self.client.registry.ready is False:
            await self.client.get_registration_status(leader_address, leader_id)
            await asyncio.sleep(1)

    async def discovered(self):
        return list(self.client.registry.discovered.values())
