import grpc
import os
import psutil
from .service import BootstrapService
from .proto import add_BootstrapServerServicer_to_server


class BootstrapServer:

    def __init__(self, config, leader_id) -> None:
        self.leader_id = leader_id

        self.session_logger.info('Initializing bootstrap server...')
        
        self._worker_threads = config.distributed_config.get(
            'max_workers',
            os.getenv(
                'MAX_THREAD_WORKERS',
                psutil.cpu_count(logical=False)
            )
        )

        self.server = grpc.aio.server()
        self.service = BootstrapService(config)

        add_BootstrapServerServicer_to_server(
            self.service,
            self.server
        )

        server_address = f'[::]:{self.service.bootstrap_port}'
        self.server.add_insecure_port(server_address)

        self.run_server = False

    async def run(self):
        self.session_logger.info(f'Bootstrap server serving on port - {self.service.bootstrap_port}')

        await self.server.start()
        self.service.running = True
        self.run_server = True

    async def stop_server(self):
        await self.service.stop(0)