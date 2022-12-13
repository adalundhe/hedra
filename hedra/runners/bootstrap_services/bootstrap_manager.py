import asyncio
import uuid
from .bootstrap_server import BootstrapServer


class BootstrapManager:

    def __init__(self, config) -> None:
        self.leader_id = uuid.uuid4()

        self.session_logger.info('Initializing bootstrap server...')

        self.server = BootstrapServer(config, self.leader_id)

    async def start_server(self):
        self.session_logger.info(f'Starting bootstrap at - {self.server.service.bootstrap_service_address}\n')
        await self.server.run()
        await self.server.service.broadkast_server.start()
        self.server.run_server = True

    async def wait(self):
        while self.server.run_server:
            await asyncio.sleep(1)

    async def stop(self):
        await self.server.stop_server()
        await self.server.service.broadkast_server.stop()

