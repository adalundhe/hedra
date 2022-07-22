import datetime
import time
from .statstream_metric import StatStreamMetric
from .metric_tags import MetricTagCollection


class PrometheusMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super(PrometheusMetric, self).__init__(
            stat=stat, 
            value=value, 
            metadata=metadata
        )
        self.format = 'prometheus'

        self.types_map = {
            'max': 'gauge',
            'min': 'gauge',
            'mean': 'gauge',
            'med': 'gauge',
            'std': 'gauge',
            'var': 'gauge',
            'mad': 'gauge'
        }

        self.metric_type = self.types_map.get(stat, 'gauge')

        self.tags = MetricTagCollection(
            tags=metadata.get('event_tags'),
            reporter_format=self.format
        )

        self.tags.add_tags([
            'metric_url:{url}'.format(
                url=metadata.get('event_url')
            ),
            'metric_host:{host}'.format(
                host=metadata.get('event_host')
            ),
            'metric_type:{request_type}'.format(
                request_type=metadata.get('event_type')
            )
        ])

        self.metric_tags = self.tags.to_dict()

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Prometheus Metric - (prometheus)

        Used with the Prometheus reporter. Metric may be specifed as:

        {metric_fields}

        The Prometheus metric will automatically map any non-count statistics
        to Prometheus Gauge metrics and will apply any non-metric information
        (metric url, metric host, metric type, tags, etc.) to labels for that
        metric.
        
        '''



        