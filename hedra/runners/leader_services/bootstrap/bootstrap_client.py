import os
from typing import List
import uuid
import grpc
from broadkast.grpc import BroadkastClient
from broadkast.discovery import Query
from hedra.runners.bootstrap_services.proto.bootstrap_pb2 import ServiceRegistrationStatusResponse
from hedra.runners.leader_services.bootstrap.discovered_leaders_registry import DiscoveredLeadersRegistry
from hedra.runners.utils.connect_timeout import connect_with_retry_async
from hedra.runners.bootstrap_services.proto import (
    BootstrapServerStub,
    ServiceRegisterRequest,
    ServiceRegistrationStatusRequest,
    ServiceRegistrationStatusResponse
)
from broadkast.grpc.proto import (
    ServiceDiscoveryResponse
)


class BoostrapClient:

    def __init__(self, config) -> None:

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

        self.registry = DiscoveredLeadersRegistry()
        self.channel = grpc.aio.insecure_channel(self.bootstrap_service_address)
        self.service = BootstrapServerStub(self.channel)
        self.broadkast_client = BroadkastClient(config=config.distributed_config)

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def register_self(self, leader_address):
        registration_request: ServiceRegistrationStatusResponse = ServiceRegisterRequest(
            service_address=leader_address,
            service_name='hedra_leader'
        )

        registration_response = await self.service.RegisterService(registration_request)

        await self.registry.register(registration_response, host=True)

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def get_registration_status(self, leader_address, leader_id):
        registration_status_request = ServiceRegistrationStatusRequest(
            service_address=leader_address,
            service_id=str(leader_id),
            service_name='hedra_leader'
        )

        status_response = await self.service.GetServiceRegistrationStatus(registration_status_request)

        await self.registry.register(status_response, host=False)

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
        
        await self.registry.register_kubernetes(discovered, host=host_address)

    

