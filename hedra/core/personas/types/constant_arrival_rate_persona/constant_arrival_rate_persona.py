import math
import time
import asyncio
import uuid
import psutil
from asyncio import Task
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.types.types import PersonaTypes
from .completed_counter import CompletedCounter


async def cancel_pending(pend: Task):
    try:
        pend.cancel()
        await pend
    
    except asyncio.CancelledError:
        pass


class ConstantArrivalPersona(DefaultPersona):

    __slots__ = (
        'completed_counter'
    )

    def __init__(self, config: Config):
        super(ConstantArrivalPersona, self).__init__(config)

        self.persona_id = str(uuid.uuid4())

        self.completed_counter = CompletedCounter()
        self.type = PersonaTypes.CONSTANT_ARRIVAL
            
    async def execute(self):

        hook_names = ', '.join([
            hook.name for hook in self._hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing {self.actions_count} Hooks: {hook_names}')

        total_time = self.total_time

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Executing for a total of - {total_time} - seconds')

        await self.start_updates()

        self.start = time.monotonic()
        completed, pending = await asyncio.wait([
            asyncio.create_task(
                self.completed_counter.execute_action(
                    self._hooks[action_idx]
                )
            ) async for action_idx in self.generator(total_time)
        ], timeout=1)

        self.end = time.monotonic()
        self.pending_actions = len(pending)

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - Execution completed with - {self.pending_actions} - actions left pending'
        )

        results = await asyncio.gather(*completed)
        
        await self.stop_updates()

        cleanup_start = time.monotonic()

        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])

        cleanup_elapsed = time.monotonic() - cleanup_start
        await self.logger.filesystem.aio['hedra.core'].info(
            f'{self.metadata_string} - Cleanup completed - Resolved {self.pending_actions} pending actions in {round(cleanup_elapsed, 2)} seconds'
        )

        for hook in self._hooks:

            session_closed_start = time.monotonic()

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Closing session - {hook.session.session_id} - for Hook - {hook.name}:{hook.hook_id}')
            await hook.session.close()

            session_closed_elapsed = time.monotonic() - session_closed_start

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Closed session - {hook.session.session_id} - for Hook - {hook.name}:{hook.hook_id}. Took: {round(session_closed_elapsed, 2)} seconds')
  
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed execution')

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
        self.completed_counter.last_batch_size = self.batch.size

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%self._hooks[action_idx].session.pool.size == 0:

                if self.completed_counter.completed_count  > 0:

                    if self.completed_counter.completed_count < self.batch.size:
                        increase_percentage = (self.batch.size - self.completed_counter.completed_count)/self.batch.size
                        increase_amount = math.ceil(increase_percentage * self.completed_counter.last_batch_size)

                        self.completed_counter.last_completed = self.completed_counter.completed_count
                        self.completed_counter.last_batch_size = self.completed_counter.last_batch_size + increase_amount

                        self._hooks[action_idx].session.extend_pool(increase_amount)
                        await asyncio.sleep(0)

                    elif self.completed_counter.completed_count > self.batch.size:
                        decrease_percentage = (self.completed_counter.completed_count - self.batch.size)/self.completed_counter.completed_count
                        decrease_amount = math.ceil(decrease_percentage * self.completed_counter.last_batch_size)

                        self.completed_counter.last_completed = self.completed_counter.completed_count
                        self.completed_counter.last_batch_size = self.completed_counter.last_batch_size - decrease_amount

                        self._hooks[action_idx].session.shrink_pool(decrease_amount)
                        await asyncio.sleep(0)
            
                    self.completed_counter.completed_count = 0

                await asyncio.sleep(self.batch.interval)

            action_idx = (action_idx + 1) % self.actions_count
            if self._hooks[action_idx].session.active%max_pool_size == 0:
                    try:
                        max_wait = total_time - elapsed
                        await asyncio.wait_for(
                            self._hooks[action_idx].session.wait_for_active_threshold(),
                            timeout=max_wait
                        )
                    except asyncio.TimeoutError:
                        pass

