from .registered_service import RegisteredService


class ServiceRegistry:

    def __init__(self, expected_leaders) -> None:
        self.services = {}

        if expected_leaders is None:
            expected_leaders = 1

        self.expected_leaders = expected_leaders

    async def register_service(self, registry_request):
        service_name = registry_request.get('service_name')
        registered_service = self.services.get(
            service_name, 
            RegisteredService(self.expected_leaders)
        )
        
        if registered_service.service_registered is False:
            self.services[service_name] = registered_service
            registered_service.service_registered = True

        await registered_service.register_node(registry_request)

        return await registered_service.get_registration_status()

    async def get_service_registration_status(self, registration_status_request):
        service_name = registration_status_request.get('service_name')
        registered_service = self.services.get(
            service_name, 
            RegisteredService(self.expected_leaders)
        )

        return await registered_service.get_registration_status()

