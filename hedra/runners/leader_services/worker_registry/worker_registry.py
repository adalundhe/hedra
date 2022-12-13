
from .registered_worker import RegisteredWorker


class WorkerRegistry:

    def __init__(self, worker_addresses, workers_count=None) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.worker_addresses = worker_addresses
        self.registered_workers = {}
        self.failed = []
        self.initial_count = workers_count if workers_count else len(worker_addresses)
        self.count = 0

    def __getitem__(self, worker_address):
        return self.registered_workers[worker_address]

    def __iter__(self):
        for worker in dict(self.registered_workers):
            yield self.registered_workers[worker]

    async def __aiter__(self):
        for worker in self.registered_workers:
            yield self.registered_workers[worker]

    async def initialize(self):
        for worker_address in self.worker_addresses:
            await self.register_worker(worker_address)

    async def register_worker(self, worker_address):
        if worker_address not in self.registered_workers:
            self.session_logger.info(f'Registering new worker from - {worker_address}.')
            self.registered_workers[worker_address] = RegisteredWorker(worker_address)
            self.count += 1

        if worker_address in self.failed:
            self.session_logger.info(f'Worker at - {worker_address} - re-registered.')
            self.failed.remove(worker_address)

        if worker_address not in self.worker_addresses:
            self.worker_addresses.append(worker_address)

    async def worker_id_not_set(self, worker_address):
        return self.registered_workers[worker_address].worker_id is None

    async def set_worker_id(self, worker_address, worker_id):
        self.registered_workers[worker_address].worker_id = worker_id

    async def remove_worker(self, worker_address):
        if worker_address in self.registered_workers and worker_address in self.worker_addresses:
            self.failed.append(worker_address)
            self.worker_addresses.remove(worker_address)
            self.count -= 1

    async def clear_registry(self):
        self.worker_addresses = []
        self.registered_workers = {}
        self.count = 0

    async def clear_failed(self):
        self.failed = []
        
    async def registered_count_equals_expected_count(self):
        return len(self.registered_workers) == self.initial_count