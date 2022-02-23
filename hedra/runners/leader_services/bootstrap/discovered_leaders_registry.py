from broadkast.grpc.proto import ServiceDiscoveryResponse
from hedra.runners.bootstrap_services.proto import ServiceRegistrationStatusResponse
from hedra.runners.leader_services.leader_registry.leader_service import LeaderService

class DiscoveredLeadersRegistry:

    def __init__(self) -> None:
        self.count = 0
        self.discovered = {}
        self.ready = False

    async def register(self, registration_response: ServiceRegistrationStatusResponse, host: str=None):
        self.count: int = registration_response.registered_count

        for node_address, node_id in zip(registration_response.registered_addresses, registration_response.registered_ids):
            self.discovered[node_address] = LeaderService(
                node_address,
                node_id,
                host=host
            )

        self.ready = registration_response.ready

    async def register_kubernetes(self, discovered: ServiceDiscoveryResponse, host: str=None):
        self.count: int = discovered.discovered_count
        
        for instance in discovered.instances:
            registered_address = instance.instance_address
            self.discovered[registered_address] = LeaderService(
                instance.instance_address, 
                instance.instance_id, 
                host=instance.instaregistrationnce_address == host
            )
            
        self.ready = True

        