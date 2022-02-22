class ServiceNode:

    def __init__(self, service_config) -> None:
        self.service_address = service_config.get('serviceAddress')
        self.service_id = service_config.get('serviceId')
        self.service_server_address = service_config.get('serviceServerAddress')
        self.service_name = service_config.get('serviceName')
