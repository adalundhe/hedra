import asyncio
from .worker_services.worker_manager import WorkerManager


class DistributedWorkerServer:

    def __init__(self, config):
        self.manager = WorkerManager(config, config.reporter_config)
        self.loop = None
        
    def register(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.manager.start_server())
        self.loop.run_until_complete(self.manager.register())

    def run(self):
        self.loop.run_until_complete(self.manager.wait())

    def kill(self):
        self.loop.run_until_complete(self.manager.stop())

