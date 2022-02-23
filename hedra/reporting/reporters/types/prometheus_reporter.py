from __future__ import annotations
import uuid
from hedra.connectors.types.prometheus_connector import PrometheusConnector as Prometheus
from .utils.tools.prometheus_tools import (
    event_to_prometheus_metric,
    metric_to_prometheus_metric
)


class PrometheusReporter:

    def __init__(self, config):
        self.format = 'prometheus'
        self.reporter_config = config
        self.connector = None
        self.session_id = str(uuid.uuid4())

    @classmethod
    def about(cls):
        return '''
        Prometheus Reporter - (prometheus)

        The Prometheus reporter allows you to submit events/metrics to Prometheus. As Prometheus allows
        for aggregation of Prometheus metrics, you may also fetch these aggregated metrics via the Prometheus 
        reporter. The Prometheus reporter utilizes the Prometheus pushgateway to submit events/metrics, and
        you must supply both the ip/port of a running pushgateway for events and/or metrics to be successfully
        submitted.

        '''

    async def init(self) -> PrometheusReporter:
        self.connector = Prometheus({
            'connection_type': 'registry',
            'job_name': self.session_id,
            **self.reporter_config
        })
        await self.connector.connect()

        return self

    async def update(self, event) -> list:
        prometheus_metric = event_to_prometheus_metric(event, self.session_id)
        await self.connector.execute(prometheus_metric)
    
        return await self.connector.commit()

    async def merge(self, connector):
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        await self.connector.execute({
            'name': key,
            'labels': {
                'type': stat_type,
                'field': stat_field ,
                'session_id': self.session_id
            }
        })
        
        return await self.connector.commit()

    async def submit(self, metric) -> PrometheusReporter:
        prometheus_metric = metric_to_prometheus_metric(metric, self.session_id)
        await self.connector.execute(prometheus_metric)
        await self.connector.commit()
        return self

    async def close(self) -> PrometheusReporter:
        await self.connector.close()
        return self