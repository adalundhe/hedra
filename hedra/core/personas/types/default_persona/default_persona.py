import time
import asyncio
import psutil
from typing import Dict, List
from easy_logger import Logger
from async_tools.functions.awaitable import awaitable
from asyncio import Task
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.personas.batching.batch import Batch
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.personas.batching import Batch
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


async def cancel_pending(pend: Task):
    try:
        pend.cancel()
        if not pend.cancelled():
            await pend

        return pend
    
    except asyncio.CancelledError as cancelled_error:
        return cancelled_error


class DefaultPersona:

    def __init__(self, config: Config):
        
        self.type = PersonaTypes.DEFAULT

        self.actions = []
        self._hooks: List[Hook] = []
        self.batch = Batch(config)

        self._live_updates = False
        self.total_time = config.total_time
        self.duration = 0
        self.total_actions = 0
        self.total_elapsed = 0
        self.start = 0
        self.end = 0
        self.completed_actions = 0
        self.pending_actions = 0
        self.completed_time = 0
        self.run_timer = False
        self.actions_count = 0

        self.is_timed = True
        self.timer_thread = None

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.loop = None
        self.current_action_idx = 0

    def setup(self, hooks: Dict[HookType, List[Hook]]):
        self._hooks = hooks.get(HookType.ACTION)
        self.actions_count = len(self._hooks)
        
            
    async def execute(self):
        hooks = self._hooks
        total_time = self.total_time

        await self.start_updates()
        
        start = time.monotonic()

        completed, pending = await asyncio.wait([
            asyncio.create_task(
                hooks[action_idx].session.execute_prepared_request(
                    hooks[action_idx].action
                )
            ) async for action_idx in self.generator(total_time)
        ], timeout=1)

        self.end = time.monotonic()
        self.start = start
        self.pending_actions = len(pending)

        results = await asyncio.gather(*completed)
        
        await self.stop_updates()

        for hook in hooks:
            await hook.session.close()

        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])
        
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        max_pool_size = self.batch.size * psutil.cpu_count(logical=False)
        action_idx = 0

        start = time.monotonic()
        while elapsed < total_time:
            yield action_idx
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start
            idx += 1

            if idx%self.batch.size == 0:
                action_idx = (action_idx + 1)%self.actions_count

            if self._hooks[action_idx].session.active%max_pool_size == 0:
                try:
                    max_wait = total_time - elapsed
                    await asyncio.wait_for(
                        self._hooks[action_idx].session.wait_for_active_threshold(),
                        timeout=max_wait
                    )
                except asyncio.TimeoutError:
                    pass

    async def start_updates(self):
        if self._live_updates:
            self.timer_thread = asyncio.create_task(self._run_timer_in_background())
    
    async def stop_updates(self):
        if self._live_updates:
            self.run_timer = False
            await self.timer_thread

    def run_updates_in_background(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_timer_in_background())

    async def _run_timer_in_background(self):
        start = time.time()
        self.run_timer = True

        while self.run_timer:
            completed_actions = 0
            self.completed_time = time.time() - start

            for deferred_batch in self.batch.deferred:
                completed_actions += await awaitable(len, deferred_batch)


            self.completed_actions = completed_actions
            await asyncio.sleep(1)

        completed_actions = 0
        for deferred_batch in self.batch.deferred:
            completed_actions += await awaitable(len, deferred_batch)

        self.completed_actions = completed_actions
        self.completed_time = time.time() - start
        