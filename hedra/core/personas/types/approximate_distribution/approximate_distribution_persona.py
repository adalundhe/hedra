import uuid
import time
import asyncio
import psutil
import math
from typing import List
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.types.types import PersonaTypes
from hedra.versioning.flags.types.unstable.flag import unstable


@unstable
class ApproximateDistributionPersona(DefaultPersona):

    __slots__ = (
        'metadata_string',
        'persona_id',
        'logger',
        'type',
        'workers',
        'actions',
        '_hooks',
        'batch',
        '_stream',
        'total_time',
        'duration',
        'total_actions',
        'total_elapsed',
        'start',
        'end',
        'completed_actions',
        'pending_actions',
        'completed_time',
        'run_timer',
        'actions_count',
        'graceful_stop',
        'is_timed',
        '_stream_thread',
        '_loop',
        'current_action_idx',
        'optimized_params',
        '_executor',
        'stream',
        'execution_metrics',
        'stream_reporter_configs',
        'stream_reporters',
        'stage_name',
        'distribution',
        'collection_interval'
    )    

    def __init__(self, config: Config):
        super().__init__(config)

        self.distribution: List[float] = config.experiment.get('distribution')
        self.collection_interval = config.experiment.get('interval_duration')
        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.APPROXIMATE_DISTRIBUTION

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        current_interval_idx = 0

        distribution = self.distribution

        intervals_count = len(distribution)
        time_interval_amount = math.floor(total_time/intervals_count)
        
        interval_periods = [
             time_interval_amount for _ in distribution
        ]

        remainder_period = total_time%intervals_count
        if remainder_period > 0:
            interval_periods[-1] += remainder_period
             

        max_pool_size = math.ceil(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
        
        interval_period = interval_periods[current_interval_idx]
        interval_time_limit = interval_period
        distribution_batch_size = distribution[current_interval_idx]

        for action in self._hooks:
            await action.session.set_pool(distribution_batch_size)

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                await asyncio.sleep(self.batch.interval)

            action_idx = (action_idx + 1)%self.actions_count
            
            if self._hooks[action_idx].session.active > max_pool_size:
                try:
                    max_wait = total_time - elapsed
                    await asyncio.wait_for(
                        self._hooks[action_idx].session.wait_for_active_threshold(),
                        timeout=max_wait
                    )
                except asyncio.TimeoutError:
                    pass

            if elapsed >= interval_time_limit and elapsed < total_time:
                
                current_interval_idx += 1
                interval_time_limit += interval_periods[current_interval_idx]
                distribution_batch_size = distribution[current_interval_idx]
       
                for action in self._hooks:
                    await action.session.set_pool(distribution_batch_size)
