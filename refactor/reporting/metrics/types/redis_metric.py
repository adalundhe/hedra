from .statstream_metric import StatStreamMetric


class RedisMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super().__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 'redis'
        self.metric_name = '{metric_name}_{stat}'.format(
            metric_name=self.metric_name,
            stat=stat
        )
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Redis Metric - (prometheus)

        Used with the Redis reporter. Metric may be specifed as:

        {metric_fields}

        Redis metrics are submitted Redis key/value pairs with the key specified as:

            <metric_name>_<metric_stat>
        
        The value is stored as serialized JSON, with tags submitted as a list of 
        dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''