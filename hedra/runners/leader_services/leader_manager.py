import asyncio
import uuid
from .leader_server import LeaderServer


class LeaderManager:

    def __init__(self, config, reporter_config) -> None:
        self.config = config
        self.reporter_config = reporter_config

        if self.leader_type == 'ephemeral-leader':
            self.job_config = config


        self.leader_id = None

        self._live_progress_updates = config.distributed_config.get('live_progress_updates', False)
        
        self.session_logger.info(f'Initializing leader...')


        self.server = LeaderServer(config)
        self.running = True

    async def register(self):

        self.session_logger.info(f'Starting leader at - {self.server.service.job_registry.electorate.leader_ip}:{self.server.service.job_registry.electorate.leader_port}.')

        await self.server.create_server(self.config)
        self.leader_id = await self.server.service.job_registry.register_leaders(self.config)
        if self.leader_id is None:
            self.leader_id = uuid.uuid4()
        
        await self.server.setup(self.leader_id)

        self.session_logger.info(f'\nWaiting for - {self.server.service.job_registry.electorate.registered_services.expected_leaders_count} - leaders.')
        await self.server.service.job_registry.wait_for_leaders()
        await self.server.run()
        
        self.session_logger.info(f'Waiting for - {self.server.service.job_registry.workers.registry.initial_count} - workers.\n')

        await self.server.leader_register_workers()         
        await self.server.service.job_registry.wait_for_workers()

        registered_count = self.server.service.job_registry.workers.registry.count
        expected_count = self.server.service.job_registry.workers.registry.initial_count

        if registered_count == expected_count:
            self.session_logger.info(f'Starting monitor...')
            if self.server.service.job_registry.monitoring is False:
                await self.server.start_monitor()

            self.session_logger.info(f'Startup complete!')

        else:
            raise Exception(f'Error: Timed out waiting for {expected_count} workers.')

    async def wait(self):

        if self.leader_type == 'ephemeral-leader':
            leader_job_id = 'ephemeral-job'

            self.session_logger.info(f'\nLeader - {self.leader_id} - starting ephemeral job.')

            await self.server.service.job_registry.create_ephemeral_job(leader_job_id, self.job_config)
            await self.server.service.job_registry.start_job(leader_job_id)

        self.server.service.job_registry.ready = True

        while self.running:
            new_worker_queue = self.server.service.new_worker_queue
            not_empty = await new_worker_queue.not_empty()

            if not_empty:
                await self.server.add_new_workers(new_worker_queue)
                await new_worker_queue.clear()
                self.server.service.new_worker_queue = new_worker_queue

            if self.leader_type == 'ephemeral-leader':
                job = self.server.service.job_registry[leader_job_id]

                if job.results_task and job.results_task.done():
                    await self.server.service.job_registry.workers.registry.clear_registry()
                    self.running = False

                    self.session_logger.info('Ephemeral job complete!\n')

                    return
            
            await self.server.service.job_registry.workers.check_worker_heartbeat()
            await self.server.service.job_registry.check_for_completed()
            await self.server.service.job_registry.update_leader_active_jobs()

            await asyncio.sleep(1)

    async def stop(self):
        await self.server.stop_server()

