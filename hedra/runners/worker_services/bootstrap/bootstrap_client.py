import os
import grpc

from broadkast.grpc import BroadkastClient
from broadkast.discovery import Query
from hedra.runners.worker_services.leader_registry import LeaderRegistry
from hedra.runners.utils.connect_timeout import connect_with_retry_async
from hedra.runners.bootstrap_services.proto import (
    BootstrapServerStub
)
from broadkast.grpc.proto import (
    ServiceDiscoveryResponse
)


class BoostrapClient:

    def __init__(self, config) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()

        self.use_bootstrap = config.distributed_config.get(
            'use_bootstrap'
        )

        self.bootsrap_ip = config.distributed_config.get(
            'bootstrap_ip',
            os.getenv('BOOSTRAP_SERVER_IP')
        )

        self.bootstrap_port = config.distributed_config.get(
            'bootstrap_port',
            os.getenv('BOOTSTRAP_SERVER_PORT', "8711")
        )

        if self.bootsrap_ip and self.bootstrap_port:
            self.bootstrap_service_address = f'{self.bootsrap_ip}:{self.bootstrap_port}'

        else:
            self.bootstrap_service_address = config.distributed_config.get(
                'bootstrap_address',
                os.getenv(
                    'BOOTSTRAP_SERVER_ADDRESS'
                )
            )

        self.registry = LeaderRegistry([])
        self.channel = grpc.aio.insecure_channel(self.bootstrap_service_address)
        self.service = BootstrapServerStub(self.channel)
        self.broadkast_client = BroadkastClient(config=config.distributed_config)

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def discover(self, host_address: str=None) -> None:
        discovered: ServiceDiscoveryResponse = await self.broadkast_client.discover_service(
            'hedra_leader',
            query=Query(
                labels=[
                    {
                        'name': 'app',
                        'value': 'hedra'
                    },
                    {
                        'name': 'role',
                        'value': 'leader'
                    }
                ]
            ),
            service_port='6669'
        )

        for instance in discovered.instances:
            await self.registry.register_leader(instance.instance_address)

    

