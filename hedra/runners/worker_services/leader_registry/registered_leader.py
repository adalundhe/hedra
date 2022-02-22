import grpc
from hedra.runners.leader_services.proto import DistributedServerStub


class RegisteredLeader:

    def __init__(self, leader_address) -> None:
        self.leader_address = leader_address
        self.leader_id = None

        channel = grpc.aio.insecure_channel(target=leader_address)
        service = DistributedServerStub(channel)

        self.client = service
