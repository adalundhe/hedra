import datetime
from .metric_tags import MetricTagCollection
from .base_metric import BaseMetric


class DataDogMetric(BaseMetric):

    fields = {
        'metric_name': str,
        'metric_value': float,
        'metric_host': str,
        'metric_stat': str,
        'metric_tags': str,
    }
    
    def __init__(self, stat=None, value=None, metadata=None):
        super(DataDogMetric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        
        self.format = 'datadog'

        stat_type = stat.lower()
        self.types_map = {
            'max': 'gauge',
            'min': 'gauge',
            'mean': 'gauge',
            'med': 'gauge',
            'std': 'gauge',
            'var': 'gauge',
            'mad': 'gauge',
            'total': 'count',
            'successful': 'count',
            'failed': 'count'
        }
        
        
        self.metric_name = '{metric_name}_{stat_type}'.format(
            metric_name=metadata.get('event_name'),
            stat_type=stat_type
        )
        self.metric_value = value
        self.metric_stat = self.types_map.get(stat_type)
        self.metric_host = metadata.get('event_host')

        self.tags = MetricTagCollection(
            tags=metadata.get('event_tags'),
            reporter_format=self.format
        )

        self.metric_tags = [
            {
                'name': 'metric_url',
                'value': metadata.get('event_url')
            },
            {
                'name': 'metric_host',
                'value': metadata.get('event_host')
            },
            {
                'name': 'metric_type',
                'value': metadata.get('event_type')
            },
            *self.tags.to_dict_list()
        ]

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Datadog Metric - (datadog)

        Used with the Datadog reporter. Metric may be specifed as:

        {metric_fields}

        Datadog metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''

    def __str__(self):
        return str(self.metric_value)

    def __repr__(self):
        return self.metric_value

                

