import time
import asyncio
import psutil
from typing import Dict, List
from easy_logger import Logger
from concurrent.futures import ThreadPoolExecutor
from async_tools.functions.awaitable import awaitable
from asyncio import Task
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.personas.batching.batch import Batch
from hedra.core.graphs.hooks.types.hook import Hook
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

    __slots__ = (
        'type',
        'workers',
        'actions',
        '_hooks',
        'batch',
        '_live_updates',
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
        'timer_thread',
        'session_logger',
        'loop',
        'current_action_idx',
        'optimized_params',
        'thread_pool'
    )    

    def __init__(self, config: Config):
        
        self.type = PersonaTypes.DEFAULT
        self.workers = 1

        self.actions = []
        self._hooks: List[Hook] = []
        self.batch = Batch(config)
        self.thread_pool = ThreadPoolExecutor(self.workers)

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
        self.graceful_stop = config.graceful_stop

        self.is_timed = True
        self.timer_thread = None

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.current_action_idx = 0
        self.optimized_params = None

    def setup(self, hooks: Dict[HookType, List[Hook]]):
        self._hooks = hooks.get(HookType.ACTION)
        self._hooks.extend(
            hooks.get(HookType.TASK, [])
        )
        self.actions_count = len(self._hooks)
        
            
    async def execute(self):
        hooks = self._hooks
        total_time = self.total_time
        loop = asyncio.get_running_loop()

        await self.start_updates()

        self.start = time.monotonic()
        completed, pending = await asyncio.wait([
            loop.create_task(
                hooks[action_idx].session.execute_prepared_request(
                    hooks[action_idx].action
                )
            ) async for action_idx in self.generator(total_time)
        ], timeout=self.graceful_stop)

        self.end = time.monotonic()
        self.pending_actions = len(pending)

        results = await asyncio.gather(*completed)
        
        await self.stop_updates()

        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])

        for hook in hooks:
            await hook.session.close()
        
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
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
        