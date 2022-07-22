from .statstream_metric import StatStreamMetric


class S3Metric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super(S3Metric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 's3'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        S3 Metric - (gcs)

        Used with the S3 reporter. Metric may be specifed as:

        {metric_fields}

        S3 metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''