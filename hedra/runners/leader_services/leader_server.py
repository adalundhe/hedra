import grpc
import os
import psutil
from .service import LeaderService
from .proto import add_DistributedServerServicer_to_server


class LeaderServer:

    def __init__(self, config):
        self.config = config
        self.ip = None
        self.port = None
  
        self.session_logger.info('Initializing leader server...')
        self._worker_threads = config.distributed_config.get(
            'max_workers', 
            os.getenv(
                'MAX_THREAD_WORKERS',
                psutil.cpu_count(logical=False)
            )
        )

        self.session_logger.debug('Generating server with - {threads} - threads'.format(threads=self._worker_threads))
        self.server = grpc.aio.server()

        self.service = LeaderService()

        add_DistributedServerServicer_to_server(
            self.service,
            self.server
        )


        self.run_server = False

    async def create_server(self, config):
        await self.service.setup_leader_addresses(config)
        self.ip = self.service.leader_ip
        self.port = self.service.leader_port

        server_address = '[::]:{port}'.format(port=self.port)
        self.server.add_insecure_port(server_address)
        await self.server.start()

    async def setup(self, leader_id):
        await self.service.setup(
            self.config,
            leader_id
        )

    async def run(self):
        self.session_logger.info('Leader server serving on port - {port}'.format(port=self.port))
        
        workers = self.service.job_registry.workers.registry.worker_addresses

        self.session_logger.info(f'Initializing with: - {workers} - workers.')
        if len(self.service.job_registry.workers.registry.worker_addresses) > 0:
            await self.service.job_registry.workers.initialize()

        self.service.running = True
        self.run_server = True

    async def stop_server(self):
        await self.server.stop(0)

    async def add_new_workers(self, new_workers_queue):
        await self.service.job_registry.workers.add_new_workers(new_workers_queue)

    async def leader_register_workers(self):
        if len(self.service.job_registry.workers.registry.worker_addresses) > 0:
            await self.service.job_registry.workers.leader_register_worker()

    async def start_monitor(self):
        await self.service.job_registry.start_job_monitor()

    async def stop_monitor(self):
        await self.service.job_registry.stop_job_monitor()

