import grpc
import os
import psutil
import asyncio
from concurrent import futures

from .service import WorkerService
from .proto import add_WorkerServerServicer_to_server

class WorkerServer:

    def __init__(self, config, reporter_config, worker_id):
        logger = Logger()
        self.loop = None
        self.worker_id = worker_id

        self.session_logger = logger.generate_logger('hedra')
        self.config = config.distributed_config
        self.worker_ip = self.config.get(
            'worker_ip',
            os.getenv(
                'WORKER_IP', 
                'localhost'
            )
        )
        self.worker_port = self.config.get(
            'worker_server_port',
            os.getenv('WORKER_SERVER_PORT',  6671)
        )
        self.address = config.distributed_config.get(
            'worker_server_address',
            os.getenv('WORKER_SERVER_ADDRESS')
        )

        if self.address is None:
            self.address = f'{self.worker_ip}:{self.worker_port}'

        leader_ips = config.distributed_config.get(
            'leader_ips',
            os.getenv('LEADER_IPS', 'localhost')
        )

        leader_ports = config.distributed_config.get(
            'leader_ports',
            os.getenv('LEADER_PORTS', "6669")
        )

        leader_addresses = config.distributed_config.get(
            'leader_addresses',
            os.getenv('LEADER_ADDRESSES')
        )

        if leader_addresses:
            leader_addresses = leader_addresses.split(",")
            self.leader_addresses = leader_addresses

        else:
            leader_ips = leader_ips.split(",")
            leader_ports = [int(port) for port in leader_ports.split(",")]

            self.leader_addresses = []

            for leader_ip, leader_port in zip(leader_ips, leader_ports):
                self.leader_addresses.append(
                    f'{leader_ip}:{leader_port}'
                )

        self.session_logger.info('Initializing updates server...')
        self._worker_threads = self.config.get(
            'max_workers', 
            os.getenv(
                'MAX_THREAD_WORKERS',
                psutil.cpu_count(logical=False)
            )
        )

        self.session_logger.debug('Generating server with - {threads} - threads'.format(threads=self._worker_threads))
        self.server = grpc.aio.server()

        self.service = WorkerService(
            config,
            reporter_config,
            self.address,
            self.worker_id
        )

        add_WorkerServerServicer_to_server(
            self.service,
            self.server
        )

        server_address = '[::]:{port}'.format(port=self.worker_port)
        self.server.add_insecure_port(server_address)

        self.run_server = False
        self.loop = None

    async def run(self):
        self.session_logger.info('Updates server serving on port - {port}'.format(port=self.worker_port))
        
        leader_address_strings = ''.join([f'\n- {leader_address}' for leader_address in self.leader_addresses])
        
        self.session_logger.info(f'\nUpdates server connecting to leaders at addresses: {leader_address_strings}\n')

        await self.service.job_registry.leaders.initialize()
        await self.server.start()
        self.service.running = True
        self.run_server = True

    async def stop_server(self):
        await self.server.stop(0)

    async def add_new_leaders(self, new_leaders_queue):
        await self.service.job_registry.leaders.add_new_leaders(new_leaders_queue)

    async def register_worker(self):
        await self.service.job_registry.leaders.register_worker()

    async def start_monitor(self):
        await self.service.job_registry.start_job_monitor()

    async def stop_monitor(self):
        await self.service.job_registry.stop_job_monitor()

