import asyncio
from concurrent.futures import ThreadPoolExecutor


class BackupVolume:

    def __init__(
        self,
        path: str,
        service_name: str,
        instance_id: str
    ) -> None:
        self.path = path
        self.service_name = service_name
        self.instance_id = instance_id

    

