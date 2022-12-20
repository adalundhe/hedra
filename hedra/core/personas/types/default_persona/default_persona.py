import time
import asyncio
import psutil
import uuid
from typing import Dict, List, Union
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.tools.helpers import awaitable
from asyncio import Task
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.personas.batching.batch import Batch
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook
)
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
        'metadata_string',
        'persona_id',
        'logger',
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
        'loop',
        'current_action_idx',
        'optimized_params',
        'thread_pool'
    )    

    def __init__(self, config: Config):
        self.persona_id = str(uuid.uuid4())
        self.logger = HedraLogger()
        self.logger.initialize()

        self.metadata_string: str = None

        self.type = PersonaTypes.DEFAULT
        self.workers = 1

        self.actions = []
        self._hooks: List[Union[ActionHook, TaskHook]] = []
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

        self.current_action_idx = 0
        self.optimized_params = None

    def setup(self, hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], metadata_string: str):

        self.metadata_string = f'{metadata_string} Persona: {self.type.capitalize()}:{self.persona_id} - '

        self._hooks = hooks.get(HookType.ACTION)
        self._hooks.extend(
            hooks.get(HookType.TASK, [])
        )
        self.actions_count = len(self._hooks)
        
            
    async def execute(self):
        hooks = self._hooks
        hook_names = ', '.join([
            hook.name for hook in hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing {self.actions_count} Hooks: {hook_names}')

        total_time = self.total_time

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Executing for a total of - {total_time} - seconds')
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

        for hook in hooks:

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
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
        action_idx = 0

        start = time.monotonic()
        while elapsed < total_time:
            yield action_idx
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start

            if self._hooks[action_idx].session.active%max_pool_size == 0:
                try:
                    max_wait = total_time - elapsed
                    await asyncio.wait_for(
                        self._hooks[action_idx].session.wait_for_active_threshold(),
                        timeout=max_wait
                    )
                except asyncio.TimeoutError:
                    pass

            action_idx = (action_idx+1)%self.actions_count

    async def start_updates(self):
        if self._live_updates:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Live Updates enabled')
            self.timer_thread = asyncio.create_task(self._run_timer_in_background())
    
    async def stop_updates(self):
        if self._live_updates:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Live Updates stopped')
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
        