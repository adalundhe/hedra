from .statstream_metric import StatStreamMetric


class GoogleCloudStorageMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super(GoogleCloudStorageMetric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 'gcs'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Google Cloud Storage Metric - (gcs)

        Used with the Google Cloud Storage reporter. Metric may be specifed as:

        {metric_fields}

        Google Cloud Storage metric tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''