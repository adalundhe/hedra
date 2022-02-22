from ctypes import Union
from typing import List
import uuid
from .leader_service import LeaderService

class LeaderRegistry:


    def __init__(self) -> None:
        self.registered = []
        self.elected_leader = None
        self.expected_leaders = []
        self.expected_leaders_count = 0
        self.registered_count = 1
        self._registered = set()
        self.leader_id = None

    async def setup(self, services: List[LeaderService]):
        self.registered = [service for service in services if service.host == True]
        self.leader_id = self.registered[0].service_id

        for leader in self.registered:
            self._registered.add(leader.service_address)

        self.expected_leaders = [service for service in services if service.host == False]
        self.expected_leaders_count = len(self.expected_leaders) + self.registered_count

    async def use_dynamic_registration(self):
        return self.expected_leaders_count > 0 and len(self.expected_leaders) == 0

    async def set_elected_leader(self, elected_leader):
        self.elected_leader = elected_leader

    async def remove_service(self, removed_address):
        self.expected_leaders = [service for service in self.expected_leaders if service.service_address != removed_address]
        if self.elected_leader.service_address == removed_address:
            self.elected_leader = None

    async def registration_complete(self):
        return self.registered_count >= self.expected_leaders_count

    async def services(self):
        for service in self.expected_leaders:
            yield service

    async def register(self, leader_registration_request):
        leader_id = uuid.UUID(leader_registration_request.host_id, version=4)        
        self.registered += [ LeaderService(leader_registration_request.host_address, leader_id) ]
        
        if leader_registration_request.host_address not in self._registered:
            self._registered.add(leader_registration_request.host_address)
            self.registered_count += 1

    async def register_by_response(self, leader_registration_response):
        leader_id = uuid.UUID(leader_registration_response.host_id, version=4)
        self.registered += [ LeaderService(leader_registration_response.host_address, leader_id) ]

        if leader_registration_response.host_address not in self._registered:
            self._registered.add(leader_registration_response.host_address)
            self.registered_count += 1


    async def find_min_id(self):        
        return min(self.registered, key=lambda registered_leader: registered_leader.service_id.int)

    async def share_job(self, job):
        pipeline_updates = []

        for leader in self.registered:
            if leader.host is False:
                updated_pipeline_status = await leader.share_current_job_config(job)

                pipeline_updates += [updated_pipeline_status]

        return pipeline_updates

    async def registered_count_equals_expected_count(self):
        return self.registered_count >= self.expected_leaders_count