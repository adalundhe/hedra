import time
import asyncio
import psutil
import uuid
import math
from typing import Dict, List, Union
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from asyncio import Task
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.personas.batching.batch import Batch
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes
from hedra.core.personas.streaming import (
    Stream,
    StreamAnalytics
)
from hedra.reporting.processed_result.results import results_types
from hedra.reporting.reporter import (
    Reporter,
    ReporterConfig
)



async def cancel_pending(pend: Task):
    try:
        if pend.done():
            pend.exception()

            return pend

        pend.cancel()
        await asyncio.sleep(0)
        if not pend.cancelled():
            await pend

        return pend
    
    except asyncio.CancelledError as cancelled_error:
        return cancelled_error

    except asyncio.TimeoutError as timeout_error:
        return timeout_error

    except asyncio.InvalidStateError as invalid_state:
        return invalid_state


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
        'streamed_analytics',
        'stream_reporter_configs',
        'stream_reporters',
        'stage_name',
        'optimization_active',
        'pending',
        'collect_analytics',
        'collection_interval',
        'bypass_cleanup'
    )    

    def __init__(self, config: Config):
        self.persona_id = str(uuid.uuid4())
        self.logger = HedraLogger()
        self.logger.initialize()

        self.metadata_string: str = None

        self.type = PersonaTypes.DEFAULT
        self.workers = 1

        self.actions = []
        self._hooks: List[Union[ActionHook, TaskHook]] = {}
        self.batch = Batch(config)
        self._loop: asyncio.AbstractEventLoop = None

        self._stream = False
        self.optimization_active = False
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
        self.stream: Stream = None 

        self.is_timed = True
        self._stream_thread = None

        self.current_action_idx = 0
        self.optimized_params = None
        self.streamed_analytics: StreamAnalytics = None
        self.stream_reporter_configs: List[ReporterConfig] = []
        self.stream_reporters: List[Reporter] = []
        self.stage_name: str = None
        self.collect_analytics: bool = False
        self.collection_interval: int = 1
        self.pending: List[asyncio.Task] = []
        self.bypass_cleanup: bool = False

    def setup(
            self, 
            hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], 
            metadata_string: str
        ):

        self._setup(
            hooks, 
            metadata_string
        )

        self.actions_count = len(self._hooks)

    def _setup(
            self, 
            hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], 
            metadata_string: str
        ):
        self.metadata_string = f'{metadata_string} Persona: {self.type.capitalize()}:{self.persona_id} - '

        actions_and_tasks = list([
            *hooks.get(HookType.ACTION),
            *hooks.get(HookType.TASK, [])
        ])

        self._hooks = [
            hook for hook in actions_and_tasks if not hook.skip
        ]

        for hook in self._hooks:
            if self.stage_name is None:
                self.stage_name = hook.stage
        
        for reporter_config in self.stream_reporter_configs:
            self.stream_reporters.append(
                Reporter(reporter_config)
            )

        self._stream = len(self.stream_reporters) > 0 and self.optimization_active is False

        if self._stream or self.collect_analytics:
            self.stream = Stream()

    async def set_concurrency(self, concurrency: int):
        for hook in self._hooks:
            await hook.session.set_pool(concurrency)

    async def execute(self):
        hooks = self._hooks
        hook_names = ', '.join([
            hook.name for hook in hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing {self.actions_count} Hooks: {hook_names}')

        total_time = self.total_time

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Executing for a total of - {total_time} - seconds')
        loop = asyncio.get_running_loop()
        if self._stream or self.collect_analytics:

            for reporter in self.stream_reporters:
                reporter.logger.filesystem.aio['hedra.reporting'].logger_enabled = False
                reporter.selected_reporter.logger.filesystem.aio['hedra.reporting'].logger_enabled = False
                await reporter.connect()

            await self.start_stream()

            self.start = time.monotonic()
            completed, pending = await asyncio.wait([
                loop.create_task(
                    self.stream.execute_action(
                        hooks[action_idx]
                    )
                ) async for action_idx in self.generator(total_time)
            ], timeout=self.graceful_stop)

            self.end = time.monotonic()

            self.streamed_analytics = await self.stop_stream()

            for reporter in self.stream_reporters:
                await reporter.close()

        else:

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
        max_pool_size = math.ceil(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
        action_idx = 0

        start = time.monotonic()
        while elapsed < total_time:
            yield action_idx
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start

            if self._hooks[action_idx].session.active > max_pool_size:
                try:
                    max_wait = total_time - elapsed
                    await asyncio.wait_for(
                        self._hooks[action_idx].session.wait_for_active_threshold(),
                        timeout=max_wait
                    )
                except asyncio.TimeoutError:
                    pass
            
            action_idx = (action_idx+1)%self.actions_count

    async def start_stream(self):
        self._loop = asyncio.get_event_loop()
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Live Updates enabled')
        self._stream_thread = asyncio.create_task(
            self._run_stream()
        )

    async def stop_stream(self) -> StreamAnalytics:
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Live Updates stopped')
        self.run_timer = False
        return await self._stream_thread

    async def _run_stream(self) -> StreamAnalytics:
        self.run_timer = True

        stream_analytics = StreamAnalytics()
        
        start = time.time()
        batch_start = time.time()
        stream_submission_tasks = []
        collection_stop_time = self.total_time - 1

        while self.completed_time < collection_stop_time and self.run_timer:
            self.completed_time = time.time() - start
            await asyncio.sleep(self.collection_interval)

            batch_elapsed = time.time() - batch_start

            stream_analytics.add(
                self.stream,
                batch_elapsed
            )

            if self._stream:
                processed_results = [
                    results_types.get(
                        result.type
                    )(
                        self.stage_name,
                        result
                    ) for result in self.stream.completed
                ]

                for reporter in self.stream_reporters:
                    stream_submission_tasks.append(
                        asyncio.create_task(
                            reporter.submit_events(processed_results)
                        )
                    )

            batch_start = time.time()
            self.stream.completed = []

        self.stream.completed = []

        if self._stream:
            await asyncio.gather(*stream_submission_tasks)
   
        self.completed_time = time.time() - start

        return stream_analytics
        