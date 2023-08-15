class InvalidServiceURLError(Exception):

    def __init__(self, url: str) -> None:
        super().__init__(
            f'Err. - {url} does not match required patter (instance_name)._(service_name)._(udp|tcp).(domain_name)'
        )