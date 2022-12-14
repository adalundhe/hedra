# import enum
# import statistics
# import multiprocessing
# import psutil
# import threading
# import asyncio
# import dill
# import time
# import ctypes
# from hedra.tools.helpers import awaitable
# from alive_progress import alive_bar
# from queue import Queue
# from hedra.command_line import CommandLine
# from hedra.reporting import Handler
# from .parallel.jobs import (
#     create_job,
#     run_job
# )


# class ParallelLocalWorker:

#     def __init__(self, config):
#         logger = Logger()
#         self.start = None
#         self.lock = threading.Lock()
#         self.session_logger = logger.generate_logger('hedra')
#         self.config = config
#         self.reporter_config = config.reporter_config
#         self.executor_config = config.executor_config
#         self._no_run_visuals = self.executor_config.get('no_run_visuals')
#         self._warmup_duration = self.executor_config.get('warmup', 0)
#         self._optimization_iters = self.executor_config.get('optimize', 0)
#         self._execute_warmup = self._warmup_duration > 0
#         self._execute_optimization = self._optimization_iters > 0
#         self._stages_completed = 0
#         self._monitor_thread = None

#         cpu_count = psutil.cpu_count()
#         self._size = self.executor_config.get(
#             'pool_size',
#             cpu_count
#         )
#         self.stage_queue = Queue(self._size)

#         if self._size < 1:
#             self.session_logger.warn(
#                 f'\nWarning - System either is single-core or requested less than one thread. Setting requested threads ot min of 1.\n'
#             )
#             self._size = 1

#         self._workers = self._size
#         self._stages = 6

#         if self._execute_warmup:
#             self._stages = 7

#         if self._execute_optimization:
#             self._stages = 8

#         self._barrier = multiprocessing.Barrier(self._workers)
#         self.stages = multiprocessing.Queue(self._stages * self._size)
#         self._current_stage = multiprocessing.Array(ctypes.c_char, 32)
#         self.optimized_aps = multiprocessing.Array(ctypes.c_float, self._workers)
#         self.optimized_batch_size = multiprocessing.Array(ctypes.c_int64, self._workers)
#         self.optimized_batch_time = multiprocessing.Array(ctypes.c_float, self._workers)
#         self._current_stage.value = b'initializing'

#         self._results = []
#         self._jobs = []
#         self._jobs_configs = []
#         self._total_actions = 0
#         self._progress_bar = None
#         self._visited = {
#             b'initializing': False,
#             b'setup': False,
#             b'warmup': False,
#             b'optimize': False,
#             b'execute': False,
#             b'results': False,
#             b'serializing': False
#         }

#     def _partition_actions(self, action=None):

#         configs = []
#         batch_size = self.config.executor_config.get('batch_size', 10**3)
#         worker_batch_size = int(batch_size/self._workers)
#         last_batch_size = worker_batch_size + (batch_size%self._workers)
#         last_batch_idx = self._workers - 1

#         with alive_bar(self._workers, title='Setting up workers...') as bar:
#             for idx, worker in enumerate(range(self._workers)):
#                 command_line = CommandLine()
#                 command_line.runner_mode = 'parallel'
#                 command_line = self.config.copy(command_line)
#                 command_line.executor_config.update(self.config.executor_config)
#                 command_line.reporter_config.update(self.config.reporter_config)
#                 command_line.log_level = self.config.log_level
#                 command_line.executor_config['pool_size'] = self._workers
#                 command_line.config_helper = None

#                 if command_line.executor_config.get('engine_type') == 'action-set':
#                     command_line.actions = {}
#                 else:
#                     command_line.actions = dict(self.config.actions)
                
#                 # command_line.executor_config['batch_size'] = batch_size
#                 if idx == last_batch_idx:
#                     command_line.executor_config['batch_size'] = last_batch_size
#                 else:
#                     command_line.executor_config['batch_size'] = worker_batch_size

#                 reporter_config = dict(self.reporter_config.copy())
#                 configs.append(dill.dumps({
#                     'config': command_line,
#                     'reporter_config': reporter_config,
#                     'worker_idx': worker
#                 }))

#                 bar()

#         return configs

#     def run(self):
#         self.session_logger.info(f'Starting parallel execution - {self._workers} - workers...\n')
#         configs = self._partition_actions(action='optimize')
#         self.session_logger.debug(f'Successfully generated bins, spawning threads...\n')
#         self.session_logger.info('\nStarting job...')

#         try:
#             self.start = time.time()
#             if self._no_run_visuals:
#                 self._run(configs)

#             else:
#                 with alive_bar(
#                     total=self._stages,
#                     title=f'Completed stages',
#                     bar=None, 
#                     spinner='dots_waves2',
#                     stats=False
#                 ) as bar:
#                     self._progress_bar = bar
#                     self._run(configs)
        
#         except Exception as err:
#             self.session_logger.error(f'\nErr - encountered exception while running parallel job - {str(err)}')
#             exit(1)


#     def _run(self, configs):
#         shared = (
#             self._barrier,
#             self.stages,
#             self._current_stage,
#             self.optimized_aps,
#             self.optimized_batch_size,
#             self.optimized_batch_time,
#         )

#         pool = multiprocessing.Pool(
#             processes=self._workers,
#             initializer=create_job,
#             initargs=shared
#         )
        
#         self._jobs = pool.map_async(run_job, configs)

#         if self._no_run_visuals:
            
#             self._results = self._jobs.get()

#         else:
#             self._stages_completed += 1
#             self._progress_bar()
#             self._progress_bar.text(f'- {self._current_stage.value.decode()}')
#             self._monitor()

#             self._results = self._jobs.get()

#             self._monitor_thread.join()

#     def _monitor(self):
#         self._monitor_thread = threading.Thread(target=self._monitor_async)
#         self._monitor_thread.start()

#     def _monitor_async(self):
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(self._monitor_progress())

#     async def _monitor_progress(self):
#         while self._stages_completed < self._stages:
#             if self._visited[self._current_stage.value] is False:
#                 self._visited[self._current_stage.value] = True
#                 await awaitable(self._progress_bar)

#                 current_stage = await awaitable(self._current_stage.value.decode)
#                 await awaitable(self._progress_bar.text, f'- {current_stage}')
#                 self._stages_completed += 1
#             await asyncio.sleep(1)
        
#         return 0
        
#     def kill(self):
#         loop = asyncio.get_event_loop()

#         handler = Handler(self.config)
#         loop.run_until_complete(handler.initialize_reporter())

#         actions_per_second_rates = []
#         total_completed_actions = []
#         elapsed_times = []
#         start_times = []
#         end_times = []

#         for result in self._results:
#             stats = result.get('stats')
#             actions_per_second_rates.append(stats.get('actions_per_second'))
#             total_completed_actions.append(stats.get('completed_actions'))
#             elapsed_times.append(stats.get('total_time'))
#             start_times.append(stats.get('start_time'))
#             end_times.append(stats.get('end_time'))

#         total_completed_actions = sum(total_completed_actions)
        
#         median_start = statistics.median(start_times)
#         median_end = statistics.median(end_times)
#         median_elapsed = median_end - median_start

#         median_actions_per_second = statistics.median(actions_per_second_rates)
#         peak_actions_per_second = sum(actions_per_second_rates)
#         average_actions_per_second = peak_actions_per_second/self._workers

#         self.session_logger.info(f'Calculated median APS per-worker of - {median_actions_per_second} - actions per second.')
#         self.session_logger.info(f'Calculated average APS per-worker of {average_actions_per_second} - actions per second.')
#         self.session_logger.info(f'Calculated estimated peak APS of - {peak_actions_per_second} - actions per second.')
#         self.session_logger.info(f'Total actions completed - {total_completed_actions} - over runtime of - {median_elapsed} - seconds')

#         self.session_logger.info(f'Submitting results for - {self._workers} - workers...')

#         for aggregated_events in self._results:
#             loop.run_until_complete(
#                 handler.merge(aggregated_events.get('events'))
#             )

#         stats = loop.run_until_complete(handler.get_stats())
#         loop.run_until_complete(handler.submit(stats))

#         self.session_logger.info('\nCompleted run, exiting...\n')
