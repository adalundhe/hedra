import asyncio
import os
import re
import uuid
import grpc
from flask import request
from hedra.servers.job_server.config_store import ConfigStore
from hedra.runners.leader_services.proto import(
    DistributedServerStub,
    NewJobLeaderRequest
)




class JobsService:

    def __init__(self, config, reporter_config):
        self.server_id = uuid.uuid4()
        self.host = None
        self.port = None
        self.leader_ips = config.distributed_config.get(
            'leader_ips',
            os.getenv('LEADER_IPS', 'localhost')
        )

        self.leader_ports = config.distributed_config.get(
            'leader_ports',
            os.getenv('LEADER_PORTS', "6669")
        )

        leader_addresses = config.distributed_config.get(
            'leader_addresses',
            os.getenv('LEADER_ADDRESSES')
        )

        if leader_addresses:
            leader_addresses = leader_addresses.split(",")
            self.leader_addresses = leader_addresses

        else:
            self.leader_ips = self.leader_ips.split(",")
            self.leader_ports = [int(port) for port in self.leader_ports.split(",")]

            self.leader_addresses = []

            for leader_ip, leader_port in zip(self.leader_ips, self.leader_ports):
                self.leader_addresses.append(
                    f'{leader_ip}:{leader_port}'
                )

        self.leaders = {}
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.session_logger.info('Initializing server...')

        self.endpoint_configs = [
            {
                'name': 'setup_job',
                'path': '/api/hedra/jobs/setup',
                'methods': ['POST', 'PUT']
            },
            {
                'name': 'delete_job',
                'path': '/api/hedra/jobs/delete',
                'methods': ['DELETE']
            },
            {
                'name': 'start_job',
                'path': '/api/hedra/jobs/start',
                'methods': ['GET']
            },
            {
                'name': 'get_job_config',
                'path': '/api/hedra/jobs/get',
                'methods': ['GET']
            }
        ]

        self.store = ConfigStore()

    def __iter__(self):
        for endpoint_config in self.endpoint_configs:
            yield endpoint_config, getattr(self, endpoint_config.get('name'))

    async def setup_job(self):
        config_json = request.json
        job_name = self.store.add_config(config_json)
        return {
            'message': 'Config setup for job - {job_name}.'.format(job_name=job_name)
        }, 201

    async def delete_job(self):
        job_name = request.args.get('job')
        self.store.delete_config(job_name)
        return {
            'message': f'Job - {job_name} deleted.'
        }, 204

    async def start_job(self):
        job_name = request.args.get('job')
        timeout = request.args.get('timeout', 300)
        
        if job_name is None:
            return {
                'message': 'Error: No job name provided.'
            }, 400

        config, reporter_config = self.store.get(job_name)

        if config is None:
            return {
                'message': 'Error: No config found for job - {job_name}'.format(
                    job_name=job_name
                )
            }, 400

        if reporter_config is None:
            return {
                'message': 'Error: No reporter config found for job - {job_name}'.format(
                    job_name=job_name
                )
            }, 400

        response = None

        leader_jobs = []
        for leader_address in self.leader_addresses:
            channel = grpc.aio.insecure_channel(leader_address)
            self.leaders[leader_address] = DistributedServerStub(channel)

        for _ in self.leaders.values():

            new_job = NewJobLeaderRequest(
                host_address=f'{self.host}:{self.port}',
                host_id=str(self.server_id),
                job_timeout=int(timeout)
            )
            new_job.job_config.update({
                'job_name': job_name,
                'executor_config': config.executor_config,
                'actions': config.actions
            })
            

            leader_jobs += [new_job]

        leader_jobs, _ = await asyncio.wait([
            leader.CreateNewJob(job) for leader, job in zip(
                self.leaders.values(),
                leader_jobs
            )
        ])

        leader_jobs = await asyncio.gather(*leader_jobs)

        for job in leader_jobs:
            if isinstance(response, grpc.RpcError):
                return {
                    'message': f'Error: Leader at - {job.host_address} - could not spawn job. Runner is busy.'
                }, 400

        return {
            'message': 'Job started.',
            'job_name': job_name
        }, 200
                
    async def get_job_config(self):
        job_name = request.args.get('job')
        config, reporter_config = self.store.get(job_name)
        if config is None:
            return {
                'message': 'Error: Config for job - {job_name} - not found.'.format(job_name=job_name)
            }, 404

        if reporter_config is None:
            return {
                'message': 'Error: Reporter config for job - {job_name} - not found.'.format(job_name=job_name)
            }, 404

        return {
            'message': 'Successfully found config for job - {job_name}'.format(job_name=job_name),
            'config': {
                'job_name': job_name,
                'executor_config': config.executor_config,
                'distributed_config': config.distributed_config,
                'reporter_config': reporter_config.reporter_config,
                'streams_config': reporter_config.stream_config
            }
        }, 200
