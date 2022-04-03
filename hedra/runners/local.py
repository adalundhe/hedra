import asyncio
from alive_progress import alive_bar
from hedra.core import Executor


class LocalWorker:

    def __init__(self, config):
        self.config = config
        self.reporter_config = config.reporter_config
        self.executor = None
        self.loop = asyncio.get_event_loop()

    def run(self, config=None):
        if config is None:
            config = self.config
        
        self.executor = Executor(config)
        self.loop.run_until_complete(self.executor.setup(self.reporter_config))
        if self.config.executor_config.get('persona_type') == 'timed' and self.config.runner_mode == 'local':
            with alive_bar() as bar:
                self.loop.run_until_complete(self.executor.generate_load())
                bar()

        else:
            self.loop.run_until_complete(self.executor.generate_load())

    def parse_results(self):
        return self.loop.run_until_complete(self.executor.serialize_results())

    def complete(self):
        aggregate_events = self.loop.run_until_complete(self.executor.calculate_results())
        return self.loop.run_until_complete(self.executor.submit_results(aggregate_events))
        