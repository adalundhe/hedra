from .registered_worker import RegisteredWorker


class NewWorkerQueue:

    def __init__(self) -> None:
        self.workers = {}
        self.worker_addresses = []

    def __iter__(self):
        for worker in self.worker_addresses:
            yield worker

    async def __aiter__(self):
        for worker in self.worker_addresses:
            yield worker

    def __getitem__(self, worker_address):
        return self.workers[worker_address]

    async def get_leaders(self):
        for worker_address in self.worker_addresses:
            yield self.workers[worker_address]

    async def add(self, new_worker_request):
        worker_address = new_worker_request.host_address
        worker_id = new_worker_request.host_id
        self.worker_addresses += [worker_address]
        
        new_worker = RegisteredWorker(worker_address)
        new_worker.worker_id = worker_id

        self.workers[worker_address] = new_worker

    async def clear(self):
        self.worker_addresses = []
        self.workers = {}

    async def not_empty(self):
        return len(self.workers) > 0
