import math
import time
import asyncio
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

    def __init__(self, config: Config):
        super(ConstantArrivalPersona, self).__init__(config)
        self.completed_counter = CompletedCounter()
        self.type = PersonaTypes.CONSTANT_ARRIVAL
            
    async def execute(self):
        total_time = self.total_time

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

        results = await asyncio.gather(*completed)
        
        await self.stop_updates()
        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])

        for hook in self._hooks:
            await hook.session.close()
  
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
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

                action_idx = (action_idx + 1) % self.actions_count
                await asyncio.sleep(self.batch.interval.period)
