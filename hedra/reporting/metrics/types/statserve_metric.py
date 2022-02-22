from .statstream_metric import StatStreamMetric


class StatServeMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super().__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 'statserve'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Statserve Metric - (statserve)

        Used with the Statserve reporter. Metric may be specifed as:

        {metric_fields}

        Statserve metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''
