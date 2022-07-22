from .base_metric import BaseMetric
from .metric_tags import MetricTagCollection


class StatStreamMetric(BaseMetric):
    fields = {
        'metric_name': str,
        'metric_value': float,
        'metric_host': str,
        'metric_url': str,
        'metric_stat': str,
        'metric_type': str,
        'metric_tags': list,
    }

    def __init__(self, stat=None, value=None, metadata=None):
        super(StatStreamMetric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )


        self.format = 'statstream'

        self.metadata = metadata
        self.metric_stat = stat
        self.metric_value = value
        
        self.metric_name = '{metric_name}_{stat}'.format(
            metric_name=metadata.get('event_name'),
            stat=self.metric_stat
        )

        self.metric_host = metadata.get('event_host')
        self.metric_url = metadata.get('event_url')
        self.metric_type = metadata.get('event_type')

        self.tags = MetricTagCollection(
            reporter_format=self.format
        )

        self.tags.add_tags(metadata.get('event_tags'))

        self.metric_tags = self.tags.to_dict_list()
        
        if metadata.get('event_context'):
            self.metric_tags.append({
                'tag_name': 'metric_context',
                'tag_value': metadata.get('event_context')
            })

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Statstream Metric - (statstream)

        Used with the Statstream reporter. Metric may be specifed as:

        {metric_fields}

        Statstream metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''

    def __str__(self):
        return str(self.metric_value)

    def __repr__(self):
        return self.metric_value