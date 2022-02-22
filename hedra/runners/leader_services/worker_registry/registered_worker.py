import grpc
from hedra.runners.worker_services.proto import (
    WorkerServerStub
)

class RegisteredWorker:

    def __init__(self, worker_address):
        self.worker_address = worker_address
        self.worker_id = None

        channel = grpc.aio.insecure_channel(worker_address)
        service = WorkerServerStub(channel)

        self.client = service
