from .registered_leader import RegisteredLeader


class NewLeaderQueue:

    def __init__(self) -> None:
        self.leaders = {}
        self.leader_addresses = []

    def __iter__(self):
        for leader in self.leader_addresses:
            yield leader

    async def __aiter__(self):
        for leader in self.leader_addresses:
            yield leader

    def __getitem__(self, leader_address):
        return self.leaders[leader_address]

    async def get_leaders(self):
        for leader_address in self.leader_addresses:
            yield self.leaders[leader_address]

    async def add(self, new_leader_request):
        leader_address = new_leader_request.host_address
        leader_id = new_leader_request.host_id
        self.leader_addresses += [leader_address]
        
        new_leader = RegisteredLeader(leader_address)
        new_leader.leader_id = leader_id

        self.leaders[leader_address] = new_leader

    async def clear(self):
        self.leader_addresses = []
        self.leaders = {}

    async def not_empty(self):
        return len(self.leaders) > 0
