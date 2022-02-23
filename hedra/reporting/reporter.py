from hedra.reporting.metrics.metric import Metric
from hedra.reporting.reporters.types.redis_reporter import RedisReporter
from .reporters import ReporterStore
from hedra.reporting.events import Event
from easy_logger import Logger


class Reporter:

    def __init__(self):
        self.log_level = None
        self.config = None
        self.reporter_store = None
        self.handler = None
        self.fields = None

    @classmethod
    def about(cls):

        reporter_types = '\n\t'.join([f'- {reporter}' for reporter in ReporterStore.reporters])

        return f'''
        Reporters


        key-arguments:

        --reporter <reporter_type_to_use_for_submitting_results> (i.e. datadog, postgres, etc. - this only applies to the submit reporter)


        Hedra offers a comprehensive suite of integrations to export and store test results, including:

        {reporter_types}

        These integrations take the form of a consistent interface via Reporters. Reporters work in three stages:
        
        - Update Reporter: Responsibile for converting individual action results and timings to Hedra Events and submitting them for storage
        - Fetch Reporter: Responsible for retrieving aggregated action timings/statistics and converting them to Hedra Metrics
        - Submit Reporter: Responsible for submitting aggregated action timing/statistics for final storage as Hedra Metrics

        The process works as follows:

        1. The result of an action is converted a Hedra Event, a consistent interface that allows for uniform specification of 
           results regardless of reporter selcted.
        
        2. Each event is submitted for storage to the location specified via the `update-reporter`.

        3. Once all events are submitted, Hedra immediately fetches the aggregated results of those events. These aggregated 
           results should represet a given "metric" for a given action (for example, avg. execution time) - the Fetch reporter 
           will convert them to a Hedra Metric. Although a given action may have many "events" (occurrences/executions), each 
           action has only *one* of a given Metric type (avg. time, pass/fail counts)

        4. Upon retrieving all metrics for all events, Hedra will submit these metrics for final storage to the location/service 
           specified via the submit reporter.
        
        You may specify different reporters for each stage (update, fetch, and submit). However, if you elect to use a different update/fetch 
        reporter than Statserve or Statstream, you must implement the aggregation for events submitted via the update reporter. If you do not have 
        external aggregation setup, we recommend leaving the update and fetch reporters as the default (Statserve) and only specifying a different 
        submit reporter.

        For more information on events, run the command:

            hedra --about results:events

        For more information on metrics, run the command:

            hedra --about results:metrics

        For more information on specific reporters, run the command:

            hedra -about results:reporting:<reporter_type>

        
        Related topics:

        - runners
        - engines 

        '''

    @classmethod
    def about_events(cls):
        return Event.about()

    @classmethod
    def about_metrics(cls):
        return Metric.about()

    @classmethod
    def about_reporter(cls, reporter_type):
        reporter = ReporterStore.reporters.get(reporter_type)
        return reporter.about()

    @classmethod
    def about_event_type(cls, event_type):
        event_type = Event.events.get(event_type)
        return event_type.about()

    @classmethod
    def about_metric_type(cls, metric_type):
        metric_type = Metric.metrics.get(metric_type)
        return metric_type.about()

    async def initialize(self, config, log_level='info'):
        self.config = config
        self.fields = config.get('fields', {})

        self.log_level = log_level

        logger = Logger()
        logger.setup(self.log_level)
        self.session_logger = logger.generate_logger()
        
        self.session_logger.debug('Initializing StatsReport...')
        self.reporter_store = ReporterStore(config=self.config)
        await self.reporter_store.init()

    async def on_event(self, data):
        
        event = Event(
            event_type=self.reporter_store.update_reporter.format,
            data=data
        )
        responses = await self.reporter_store.update(event)

        for response in responses:
            stream_field = response.get('field')

            if self.fields.get(stream_field) is None and stream_field:
                self.fields[stream_field] = {}

    async def on_events(self, data):
        events = [
            Event(
                event_type=self.reporter_store.update_reporter.format,
                data=event
            ) for event in data
        ]

        responses = await self.reporter_store.update_stream(events)

        for response in responses:
            stream_field = response.get('field')

            if self.fields.get(stream_field) is None and stream_field:
                self.fields[stream_field] = {}

    async def on_output(self, stat_type=None, stat_field=None, partial=False):
        for field in self.fields:
            metrics = await self.reporter_store.fetch(
                key=field,
                stat_type=stat_type,
                stat_field=stat_field,
                partial=partial
            )

            for metric in metrics:
                await self.reporter_store.submit(metric)

        await self.reporter_store.close()
    

       

