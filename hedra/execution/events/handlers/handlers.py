import math
import asyncio
from easy_logger import Logger
from hedra.reporting import Reporter
from .responses import Response
from async_tools.functions import awaitable
from async_tools.datatypes import AsyncList


class Handler:

    def __init__(self, config):
        self.config = config
        self.runner_mode = self.config.runner_mode
        self.reporter = Reporter()
        self.batches = 0

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')


    @classmethod
    def about(cls):
        return '''
        Results

        key-arguments:

        --reporter-config-filepath <path_to_reporter_config_JSON_file> (required but defaults to the directory in which hedra is running)

        Hedra will automatically process results at the end of execution based on the configuration stored in
        and provided by the config.json. For exmaple, a file title reporter-config.json containing:

        {
            "reporter_config": {
                "update_connector_type": "statserve",
                "fetch_connector_type": "statserve",
                "submit_connector_type": "statserve",
                "update_config": {
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                },
                "fetch_config": {
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                },
                "submit_config": {
                    "save_to_file": true,
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                }
            }
        }

        will tell Hedra to use Statserve for results aggregation/computation and then output a JSON file of aggregated 
        metrics and results at the end.

        Depdending on the executor seleted, Hedra will either attempt to connect to the specified reporter resources
        prior to execution or (specifically for the parallel executor) after execution has completed. For more information
        on how reporters work, run the command:

            hedra --about results:reporting

        Related Topics:

        - runners
        
        '''


    async def on_config(self, reporter_config):
        await self.reporter.initialize(reporter_config, log_level=self.config.log_level)

    async def on_events(self, events, serialize=True):
        if serialize:
            events = [action async for action in self._serialize_action(events)]

        await asyncio.gather(*[self.reporter.on_events(events_batch) async for events_batch in self._results_batches(events)])

    async def on_exit(self):
        if self.runner_mode == 'worker':
            self.session_logger.warning('Warning: Results cannot be generated for distributed workers.')
        else:
            await self.reporter.on_output()

        return True

    async def serialize(self, actions):
        parsed_actions = []
        async for action in AsyncList(actions):
            parsed_batch = await self._serialize_action(action)
            parsed_actions.append(parsed_batch)
        return parsed_actions

    async def _serialize_action(self, actions):
        for action in actions:
            response = Response(action)
            await response.assert_response()
            response_dict = await response.to_dict()
            yield response_dict

    async def _results_batches(self, results):
        self.batches = int(math.sqrt(len(results)))
        for i in range(0, len(results), self.batches):
            yield results[i:i + self.batches]
            