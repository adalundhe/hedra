import statistics
import multiprocessing
import psutil
import threading
import asyncio
import dill
import time
import ctypes
from zebra_async_tools.functions import awaitable
from alive_progress import alive_bar
from queue import Queue
from zebra_automate_logging import Logger
from hedra.command_line import CommandLine
from hedra.execution.events.handlers import Handler
from hedra.parsing import ActionsParser
from .parallel.jobs import (
    create_job,
    run_job
)


class ParallelLocalWorker:

    def __init__(self, config):
        logger = Logger()
        self.start = None
        self.lock = threading.Lock()
        self.session_logger = logger.generate_logger('hedra')
        self.config = config
        self.reporter_config = config.reporter_config
        self.executor_config = config.executor_config
        self._no_run_visuals = self.executor_config.get('no_run_visuals')
        self._warmup_duration = self.executor_config.get('warmup', 0)
        self._optimization_iters = self.executor_config.get('optimize', 0)
        self._execute_warmup = self._warmup_duration > 0
        self._execute_optimization = self._optimization_iters > 0
        self._stages_completed = 0
        self._monitor_thread = None

        cpu_count = psutil.cpu_count(logical=False)
        self._size = self.executor_config.get(
            'pool_size',
            cpu_count
        )
        self.stage_queue = Queue(self._size)

        if self._size < 1:
            self.session_logger.warn(
                f'\nWarning - System either is single-core or requested less than one thread. Setting requested threads ot min of 1.\n'
            )
            self._size = 1

        self._workers = self._size
        self._stages = 6

        if self._execute_warmup:
            self._stages = 7

        if self._execute_optimization:
            self._stages = 8

        self._barrier = multiprocessing.Barrier(self._workers)
        self.stages = multiprocessing.Queue(self._stages * self._size)
        self._current_stage = multiprocessing.Array(ctypes.c_char, 32)
        self.optimized_aps = multiprocessing.Array(ctypes.c_float, self._workers)
        self.optimized_batch_size = multiprocessing.Array(ctypes.c_int64, self._workers)
        self.optimized_batch_time = multiprocessing.Array(ctypes.c_float, self._workers)
        self._current_stage.value = b'initializing'

        self._results = []
        self._jobs_configs = []
        self._total_actions = 0
        self._progress_bar = None
        self._visited = {
            b'initializing': False,
            b'setup': False,
            b'warmup': False,
            b'optimize': False,
            b'execute': False,
            b'results': False,
            b'serializing': False
        }
        self.handler = Handler(config)
        actions_parser = ActionsParser(config)
        
        loop = asyncio.get_event_loop()
        parsed_actions = loop.run_until_complete(actions_parser.parse())
        self.reporter_fields = [action.name for action in parsed_actions]

    def _partition_actions(self, action=None):

        configs = []
        with alive_bar(self._workers, title='Setting up workers...') as bar:
            for worker in range(self._workers):
                command_line = CommandLine()
                command_line = self.config.copy(command_line)
                command_line.executor_config.update(self.config.executor_config)
                command_line.reporter_config.update(self.config.reporter_config)
                command_line.log_level = self.config.log_level
                command_line.executor_config['pool_size'] = self._workers
                command_line.actions = {}
                command_line.config_helper = None
                
                reporter_config = dict(self.reporter_config.copy())
        
                configs.append(dill.dumps({
                    'config': command_line,
                    'reporter_config': reporter_config,
                    'worker_idx': worker
                }))
                bar()

        return configs

    def run(self):
        self.session_logger.info(f'Starting parallel worker - {self._workers} - workers...\n')
        configs = self._partition_actions(action='optimize')
        self.session_logger.debug(f'Successfully generated bins, spawning threads...\n')
        self.session_logger.info('\nStarting job...')

        try:
            self.start = time.time()
            if self._no_run_visuals:
                self._run(configs)

            else:
                with alive_bar(
                    total=self._stages,
                    title=f'Completed stages',
                    bar=None, 
                    spinner='dots_waves2',
                    stats=False
                ) as bar:
                    self._progress_bar = bar
                    self._run(configs)
        
        except Exception as err:
            self.session_logger.error(f'\nErr - encountered exception while running parallel job - {str(err)}')
            exit(1)

    def _run(self, configs):
        pool = multiprocessing.Pool(
            self._workers, 
            initializer=create_job, 
            initargs=(
                self._barrier,
                self.stages,
                self._current_stage,
                self.optimized_aps,
                self.optimized_batch_size,
                self.optimized_batch_time,
            )
        )

        self._results = pool.map_async(run_job, configs, )

        if self._no_run_visuals:
            self._results = self._results.get()

        else:
            self._stages_completed += 1
            self._progress_bar()
            self._progress_bar.text(f'- {self._current_stage.value.decode()}')
            self._monitor()
            self._results = self._results.get()
            self._monitor_thread.join()

    def _monitor(self):
        self._monitor_thread = threading.Thread(target=self._monitor_async)
        self._monitor_thread.start()

    def _monitor_async(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._monitor_progress())

    async def _monitor_progress(self):
        while self._stages_completed < self._stages:
            if self._visited[self._current_stage.value] is False:
                self._visited[self._current_stage.value] = True
                await awaitable(self._progress_bar)

                current_stage = await awaitable(self._current_stage.value.decode)
                await awaitable(self._progress_bar.text, f'- {current_stage}')
                self._stages_completed += 1
            await asyncio.sleep(1)
        
        return 0
        
    def kill(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.handler.on_config(self.reporter_config))

        actions_per_second_rates = []
        total_completed_actions = []
        elapsed_times = []
        start_times = []
        end_times = []

        for result in self._results:
            stats = result.get('stats')
            actions_per_second_rates.append(stats.get('actions_per_second'))
            total_completed_actions.append(stats.get('completed_actions'))
            elapsed_times.append(stats.get('total_time'))
            start_times.append(stats.get('start_time'))
            end_times.append(stats.get('end_time'))

        total_completed_actions = sum(total_completed_actions)

        true_start = min(start_times)
        true_end = max(end_times)
        true_elapsed = true_end - true_start

        median_elapsed_time = statistics.median(elapsed_times)
        median_actions_per_second = statistics.median(actions_per_second_rates)
        average_actions_per_second = sum(actions_per_second_rates)/self._workers
        peak_actions_per_second = total_completed_actions/median_elapsed_time

        self.session_logger.info('\n')
        self.session_logger.info(f'Calculated median APS per-worker of - {median_actions_per_second} - actions per second.')
        self.session_logger.info(f'Calculated average APS per-worker of {average_actions_per_second} - actions per second.')
        self.session_logger.info(f'Calculated estimated peak APS of - {peak_actions_per_second} - actions per second.')
        self.session_logger.info(f'Total action completed - {total_completed_actions} over actual runtime of - {median_elapsed_time} - seconds')
        self.session_logger.info(f'Total actions completed - {total_completed_actions} - over wall-clock runtime of - {true_elapsed} - seconds')

        self.session_logger.info(f'Submitting results for - {self._workers} - workers...\n')
        with alive_bar(
            title='Processing results...',
            bar=None, 
            monitor=False,
            spinner='dots_waves2',
            stats=False
        ) as bar:
            for result in self._results:
                loop.run_until_complete(
                    self.handler.on_events(result.get('events'), serialize=False)
                )

        loop.run_until_complete(self.handler.on_exit())

        self.session_logger.info('\nCompleted run, exiting...\n')
