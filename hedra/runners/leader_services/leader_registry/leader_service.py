
from google.protobuf.json_format import MessageToDict
import grpc
from hedra.runners.utils.connect_timeout import connect_or_return_none
from hedra.runners.leader_services.proto import (
    DistributedServerStub,
    LeaderRegisterRequest,
    PollLeaderRequest,
    LeaderJobRequest
)


class LeaderService:


    def __init__(self, service_address, service_id, host=False) -> None:
        self.service_address = service_address
        self.service_id = service_id
        self.channel = grpc.aio.insecure_channel(service_address)
        self.service = DistributedServerStub(self.channel)
        self.active = True
        self.host = host

    async def is_registered(self, registered_services):
        return self.service_address in registered_services.registered

    @connect_or_return_none(wait_buffer=1, timeout_threshold=3)
    async def register(self, service_address, service_id):
        request = LeaderRegisterRequest(
                host_address=service_address,
                host_id=str(service_id)
            )

        return await self.service.RegisterLeader(request)

    @connect_or_return_none(wait_buffer=1, timeout_threshold=3)
    async def poll(self):
        request = PollLeaderRequest(
            host_address=self.service_address,
            host_id=str(self.service_id)
        )
        return await self.service.PollLeader(request)

    @connect_or_return_none(wait_buffer=5, timeout_threshold=15)
    async def share_current_job_config(self, job):

        request = LeaderJobRequest(
            host_address=self.service_address,
            host_id=str(self.service_id),
            job_id=job.job_id
        )

        request.job_config.update(job.config)
        request.pipeline_status.update(job.pipeline.reported_completions)

        response = await self.service.ShareCurrentJob(request) 
        updated_pipeline_status = MessageToDict(response.pipeline_status)

        return updated_pipeline_status

           
