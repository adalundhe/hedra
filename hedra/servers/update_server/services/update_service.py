import asyncio
import os
import grpc
from hedra.tools.data_structures import AsyncList, AsyncDict
from hedra.tools.helpers import awaitable
from hedra.runners.leader_services.proto import (
    WorkerUpdateRequest,
    DistributedServerStub
)
from google.protobuf.json_format import (
    MessageToDict
)

from hedra.command_line import CommandLine
from hedra.runners.utils.connect_timeout import connect_with_retry

class UpdateService:

    def __init__(self, command_line=None):
        logger = Logger()
        if command_line is None:
            command_line = CommandLine()
            
        self.config = command_line.distributed_config
        self.session_logger = logger.generate_logger('hedra')
        leader_ips = command_line.distributed_config.get(
            'leader_ips',
            os.getenv('LEADER_IPS', 'localhost')
        )

        leader_ports = command_line.distributed_config.get(
            'leader_ports',
            os.getenv('LEADER_PORTS', "6669")
        )

        leader_addresses = command_line.distributed_config.get(
            'leader_addresses',
            os.getenv('LEADER_ADDRESSES')
        )

        self.leader_ips = {}
        self.leader_ports = {}

        if leader_addresses:
            leader_addresses = leader_addresses.split(",")
            self.leader_addresses = leader_addresses
            for leader_address in self.leader_addresses:
                leader_ip, leader_port = leader_address.split(':')
                self.leader_ips[leader_address] = leader_ip
                self.leader_ports[leader_address] = int(leader_port)

        else:
            leader_ips = leader_ips.split(",")
            leader_ports = [int(port) for port in leader_ports.split(",")]

            self.leader_addresses = []

            for leader_ip, leader_port in zip(leader_ips, leader_ports):
                leader_address = f'{leader_ip}:{leader_port}'
                self.leader_addresses.append(leader_address)

                self.leader_ips[leader_address] = leader_ip
                self.leader_ports[leader_address] = leader_port

        self.channels = {}
        self.services = {}

        for leader_address in self.leader_addresses:
            channel = grpc.aio.insecure_channel(leader_address)

            self.channels[leader_address] = channel
            self.services[leader_address] = DistributedServerStub(channel)

        self.services = AsyncDict(self.services)

        self.endpoint_configs = [
            {
                'name': 'get_worker_updates',
                'path': '/api/hedra/workers/updates',
                'methods': ['GET']
            }
        ]

        self.loop = asyncio.get_event_loop()

    def __iter__(self):
        for endpoint_config in self.endpoint_configs:
            yield endpoint_config, getattr(self, endpoint_config.get('name'))

    def get_worker_updates(self):
        update_response = self.loop.run_until_complete(self.GetUpdate())
        return {
            'data': update_response
        }, 200

    @connect_with_retry(timeout_threshold=15)
    async def GetUpdate(self):
        updates = AsyncList()

        async for service_address, service in self.services.iter_items():
            leader_ip = self.leader_ips[service_address]
            leader_port = self.leader_ports[service_address]

            update_request = WorkerUpdateRequest(
                leader_address=leader_ip,
                leader_port=leader_port
            )
            update_response = await service.GetUpdate(update_request)
            update_response = MessageToDict(update_response)
            
            await updates.append(update_response)

        return updates.data




