import os
from .service_registry import ServiceRegistry
import uuid
from .proto import (
    ServiceRegisterRequest,
    ServiceRegistrationStatusRequest,
    ServiceRegistrationStatusResponse,
    BootstrapServerServicer,

)

from broadkast.grpc import BroadkastServer
from google.protobuf.json_format import (
    MessageToDict
)


class BootstrapService(BootstrapServerServicer):

    def __init__(self, config) -> None:
        super().__init__()

        self.bootstrap_service_id = str(uuid.uuid4())

        self.bootstrap_ip = config.distributed_config.get(
            'bootstrap_ip',
            os.getenv('BOOSTRAP_SERVER_IP')
        )

        self.bootstrap_port = config.distributed_config.get(
            'bootstrap_port',
            os.getenv('BOOTSTRAP_SERVER_PORT', "8711")
        )

        self.bootstrap_service_address = config.distributed_config.get(
            'bootstrap_address',
            os.getenv(
                'BOOTSTRAP_SERVER_ADDRESS', 
                f'{self.bootstrap_ip}:{self.bootstrap_port}'
            )
        )

        expected_leaders = int(config.distributed_config.get(
            'leaders', 
            os.getenv('LEADERS', 0)
        ))

        self.registry = ServiceRegistry(expected_leaders)
        self.broadkast_server = BroadkastServer(config=config.distributed_config)

        self.running = True

    async def RegisterService(self, request, context):
        register_request = MessageToDict(request)
        
        registration_response = await self.registry.register_service(register_request)

        service_registration_status_response = ServiceRegistrationStatusResponse(
            bootstrap_service_address=self.bootstrap_service_address,
            bootstrap_service_id=self.bootstrap_service_id,
            registered_count=registration_response.get('registered_count'),
            ready=registration_response.get('ready')
        )

        service_registration_status_response.registered_addresses.extend(
            registration_response.get('registered_addresses')
        )

        service_registration_status_response.registered_ids.extend(
            registration_response.get('registered_ids')
        )

        service_registration_status_response.registered_server_addresses.extend(
            registration_response.get('registered_server_addresses')
        )

        return service_registration_status_response

    async def GetServiceRegistrationStatus(self, request, context):

        registration_status_request = MessageToDict(request)
        
        service_registration_status = await self.registry.get_service_registration_status(
            registration_status_request
        )

        service_registration_status_response = ServiceRegistrationStatusResponse(
            bootstrap_service_address=self.bootstrap_service_address,
            bootstrap_service_id=self.bootstrap_service_id,
            registered_count=service_registration_status.get('registered_count'),
            ready=service_registration_status.get('ready')
        )

        service_registration_status_response.registered_addresses.extend(
            service_registration_status.get('registered_addresses')
        )

        service_registration_status_response.registered_ids.extend(
            service_registration_status.get('registered_ids')
        )

        service_registration_status_response.registered_server_addresses.extend(
            service_registration_status.get('registered_server_addresses')
        )

        return service_registration_status_response
