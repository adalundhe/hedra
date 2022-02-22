from .statstream_metric import StatStreamMetric
from .metric_tags import MetricTagCollection


class PostgresMetric(StatStreamMetric):
    fields = {
        'metric_name': str,
        'metric_value': float,
        'metric_host': str,
        'metric_url': str,
        'metric_stat': str,
        'metric_type': str,
        'metric_tags': str,
    }

    def __init__(self, stat=None, value=None, metadata=None):
        super(PostgresMetric, self).__init__(
            stat=stat,
            value=value,
            metadata=metadata
        )

        self.format = 'postgres'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        metric_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Postgres Metric - (postgres)

        Used with the Postgres reporter. Metric may be specifed as:

        {metric_fields}

        Postgres metric tags are submitted as a list of Postgres records to
        their own table with:

        - tag_name: name of the tag
        - tag_value: tag value

        as table fields.
        
        '''