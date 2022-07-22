from .statstream_metric import StatStreamMetric


class SnowflakeMetric(StatStreamMetric):

    def __init__(self, stat=None, value=None, metadata=None):
        super(SnowflakeMetric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.format = 'snowflake'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Snowflake Metric - (snowflake)

        Used with the Snowflake reporter. Metric may be specifed as:

        {metric_fields}

        Snowflake metric tags are submitted as a list of Snowflake records to
        their own table with:

        - tag_name: name of the tag
        - tag_value: tag value

        as table fields.
        
        '''