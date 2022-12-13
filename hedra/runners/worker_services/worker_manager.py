import asyncio
import os
import uuid

from .worker_server import WorkerServer


class WorkerManager:

    def __init__(self, config, reporter_config) -> None:
        logger = Logger()
        
        self.worker_type = config.runner_mode
        self.ephemeral_job_name = 'ephemeral-job'
  
        self.session_logger = logger.generate_logger()
        self.worker_id = uuid.uuid4()

        self.worker_ip = config.distributed_config.get(
            'worker_ip',
            os.getenv('WORKER_IP', 'localhost')
        )
        self.worker_port = config.distributed_config.get(
            'worker_port',
            os.getenv('WORKER_PORT', 6670)
        )
        self.worker_server_port = config.distributed_config.get(
            'worker_server_port',
            os.getenv('WORKER_SERVER_PORT',  6671)
        )

        self.worker_address = f'{self.worker_ip}:{self.worker_port}'
        self._live_progress_updates = config.distributed_config.get('live_progress_updates', False)
        
        self.session_logger.info(f'Initializing worker at - {self.worker_address}')


        self.server = WorkerServer(config, reporter_config, self.worker_id)
        self.running = True

    async def register(self):
        self.session_logger.info(f'\nStarting worker at - {self.worker_ip}:{self.worker_server_port}.')
        self.session_logger.info(f'Waiting for - {self.server.service.job_registry.leaders.registry.initial_count} - leaders.')
        await self.server.register_worker()
        await self.server.service.job_registry.wait_for_leaders()

        self.session_logger.info(f'\nStarting monitor...')
        if self.server.service.job_registry.monitoring is False:
            await self.server.start_monitor()
        
        self.session_logger.info(f'Startup complete!')

    async def run(self):
        self.session_logger.debug('Live updates? - {updates}'.format(
            updates=self._live_progress_updates
        ))
        
        await self.start_server()

    async def start_server(self):
        self.session_logger.debug('Running worker server...')
        await self.server.run()


    async def wait(self):
        while self.running:

            new_leader_queue = self.server.service.new_leader_queue
            not_empty = await new_leader_queue.not_empty()

            if not_empty:
                await self.server.add_new_leaders(new_leader_queue)
                await new_leader_queue.clear()
                self.server.service.new_leader_queue = new_leader_queue
            
            await self.server.service.job_registry.leaders.check_leader_heartbeat()
            await self.server.service.job_registry.update_job_leaders()

            if self.worker_type == 'ephemeral-worker':
                job = self.server.service.job_registry[self.ephemeral_job_name]
                if job:
                    if job.status == 'completed' or job.status == 'failed':
                        self.running = False
                        return

            await asyncio.sleep(1)

    async def stop(self):
        await self.server.stop_server()

