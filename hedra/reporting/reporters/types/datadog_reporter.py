from __future__ import annotations
import datetime
import time
from hedra.reporting.connectors.types.datadog_connector import DatadogConnector as Datadog
from easy_logger import Logger


class DatadogReporter:

    def __init__(self, config):
        self.format = 'datadog'
        self.reporter_config = config
        self.connector = Datadog(self.reporter_config)
        self._start_time = None

    @classmethod
    def about(cls):
        return '''
        Datadog Reporter - (datadog)

        The Datadog reporter allows you to submit events and metrics to Datadog for aggregation and viewing. Note
        that if no dashboard exists or if an existing dashboard needs to be re-configured to display events/metrics,
        you may do so by passing dashboard configuration options (see Connectors for more information). 
        Dashboards should be based as key/value pairs with keys of "events_dashboard" and/or "metrics_dashboard",
        with each key having a value of a dictionary/JSON object containing a valid dashboard configuration. Also
        note that event/metric tags are particularly handy when aggregating events via Datadog.

        Config Options:

        {
            "datadog_api_key": "<datadog_api_key>",
            "datadog_app_key": "<datadog_app_key>",
            "datadog_statsd_host": "<statsd_ip>", (optional)
            "datadog_statsd_socket": "<statsd_socket_port>", (optional)
            "dashboard_config": {
                ...<dashboard_config> (optional)
            },
            "events_dashboard": {
                ...<events_dashboard_config> (optional)
            },
            "metrics_dashboard": {
                ...<metrics_dashboard_config> (optional)
            }
        }

        '''

    async def init(self) -> DatadogReporter:
        await self.connector.connect()
        self._start_time = time.mktime(
            datetime.datetime.now().timetuple()
        )

        if self.reporter_config.get('dashboard_config'):
            dashboard_config = self.reporter_config.get('dashboard_config')
            metrics_dashboard = dashboard_config.get('metrics_dashboard')
            
            if dashboard_config.get('id') is None:
                action = 'create'
            else:
                action = 'update'

            await self.connector.execute({
                'type': 'dashboard',
                'action': action
                **metrics_dashboard
            })

    async def submit(self, metric) -> DatadogReporter:
        await self.connector.execute({
            'type': 'metric',
            'action': 'create',
            'name': metric.metric.metric_name,
            'values': [metric.metric.metric_value],
            'host': metric.metric.metric_host,
            'tags': metric.metric.metric_tags,
            'metric_type': metric.metric.metric_stat
        })

        return self

    async def close(self) -> DatadogReporter:
        await self.connector.clear()
        await self.connector.close()
        return self
            