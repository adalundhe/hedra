from .statstream_metric import StatStreamMetric


class MongoDBMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super().__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 'mongodb'
        self.tags.update_format(reporter_format=self.tags)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        MongoDB Metric - (mongodb)

        Used with the MongoDB reporter. Metric may be specifed as:

        {metric_fields}

        MongoDB metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''