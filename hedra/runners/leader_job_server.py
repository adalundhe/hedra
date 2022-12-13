import asyncio
from .leader_services.leader_manager import LeaderManager


class LeaderJobServer:

    def __init__(self, config) -> None:
        self.manager = LeaderManager(config, config.reporter_config)
        self.loop = None
        
    def register(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.manager.register())

    def run(self):
        self.loop.run_until_complete(self.manager.wait())

    def kill(self):
        self.loop.run_until_complete(self.manager.stop())