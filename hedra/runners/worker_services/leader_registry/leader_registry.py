
from hedra.runners.leader_services.proto import DistributedServerStub
from .registered_leader import RegisteredLeader


class LeaderRegistry:

    def __init__(self, leader_addresses) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.leader_addresses = leader_addresses
        self.registered_leaders = {}
        self.failed = []
        self.initial_count = len(leader_addresses)
        self.count = 0

    def __getitem__(self, leader_address):
        return self.registered_leaders[leader_address]

    def __iter__(self):
        for leader in self.registered_leaders:
            yield self.registered_leaders[leader]

    async def __aiter__(self):
        for leader in self.registered_leaders:
            yield self.registered_leaders[leader]

    async def initialize(self):
        for leader_address in self.leader_addresses:
            await self.register_leader(leader_address)

    async def register_leader(self, leader_address):
        if leader_address not in self.registered_leaders:
            self.session_logger.info(f'Registering leader from - {leader_address}.')
            self.registered_leaders[leader_address] = RegisteredLeader(leader_address)
            self.count += 1

        if leader_address in self.failed:
            self.session_logger.info(f'Leader at - {leader_address} - re-registered.')
            self.failed.remove(leader_address)

        if leader_address not in self.leader_addresses:
            self.leader_addresses.append(leader_address)

    async def leader_id_not_set(self, leader_address):
        return self.registered_leaders[leader_address].leader_id is None

    async def set_leader_id(self, leader_address, leader_id):
        self.registered_leaders[leader_address].leader_id = leader_id

    async def remove_leader(self, leader_address):
        
        if leader_address in self.registered_leaders and leader_address in self.leader_addresses:
            self.failed.append(leader_address)
            self.leader_addresses.remove(leader_address)
            self.count -= 1

    async def registered_count_equals_expected_count(self):
        return len(self.registered_leaders) == len(self.leader_addresses)

                
        