from .statstream_metric import StatStreamMetric
from .metric_tags import MetricTagCollection


class KafkaMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super().__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.metric_tags = self.tags.to_string_list()
        self.format = 'kafka'
        self.tags.update_format(reporter_format=self.format)

    def to_format(self):
        return {    
            'message_value': {
                'message_topic': self.metric_name,
                'metric_value': self.metric_value,
                'metric_stat': self.metric_stat,
                'metric_host': self.metric_host,
                'metric_type': self.metric_type,
                'metric_url': self.metric_url,
                'message_tags': self.metric_tags
            }
        }

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Kafka Metric - (prometheus)

        Used with the Kafka reporter. Metric may be specifed as:

        {metric_fields}

        Kafka metrics are submitted Kafka messages to the specified metrics
        topic as serialized JSON, with tags submitted as a list of dictionaries 
        with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''