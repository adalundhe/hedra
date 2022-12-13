import imp


import uuid
from .service_node import ServiceNode


class RegisteredService:

    def __init__(self, expected_leaders) -> None:

        self.service_id = str(uuid.uuid4())
        self.registered_nodes = {}
        self.registered_count = 0
        self.requested_count = expected_leaders
        self.service_registered = False

    async def register_node(self, service_config):

        new_service_node = ServiceNode(service_config)

        is_registered = self.registered_nodes.get(new_service_node.service_address)

        if is_registered is None:
            self.registered_nodes[new_service_node.service_address] = new_service_node
            self.registered_count = len(self.registered_nodes)

            self.session_logger.info(f'Registered - {new_service_node.service_address} - for service - {new_service_node.service_name}.')

        else:
            self.session_logger.info(f'Re-registered - {new_service_node.service_address} - for service - {new_service_node.service_name}.')


    async def get_registration_status(self):
        self.registered_count = len(self.registered_nodes)
        
        return {
            'registered_addresses': list(self.registered_nodes.keys()),
            'registered_ids': [registered_node.service_id for registered_node in self.registered_nodes.values()],
            'registered_server_addresses': [registered_node.service_server_address for registered_node in self.registered_nodes.values()],
            'registered_count': self.registered_count,
            'ready': self.registered_count == self.requested_count
        }

