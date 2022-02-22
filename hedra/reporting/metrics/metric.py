import datetime
import tzlocal
from .types import (
    DataDogMetric,
    StatServeMetric,
    CassandraMetric,
    MongoDBMetric,
    PostgresMetric,
    PrometheusMetric,
    RedisMetric,
    KafkaMetric,
    S3Metric,
    GoogleCloudStorageMetric,
    SnowflakeMetric,
    StatStreamMetric
)


class Metric:

    metrics = {
        'datadog': DataDogMetric,
        'statserve': StatServeMetric,
        'cassandra': CassandraMetric,
        'mongodb': MongoDBMetric,
        'postgres': PostgresMetric,
        'prometheus': PrometheusMetric,
        'redis': RedisMetric,
        'kafka': KafkaMetric,
        's3': S3Metric,
        'gcs': GoogleCloudStorageMetric,
        'snowflake': SnowflakeMetric,
        'statstream': StatStreamMetric
    }

    def __init__(self, reporter_type=None, stat=None, value=None, metadata=None):
        self.metric = self.metrics.get(
            reporter_type, 'statserve'
        )(
            stat=stat,
            value=value,
            metadata=metadata
        )
        self.fields = self.metric.fields
        self.tags = self.metric.tags
        self.format = self.metric.format
        self.value = self.metric.metric_value
        self.time = datetime.datetime.now()
        self.machine_timezone = tzlocal.get_localzone()

    @classmethod
    def about(cls):

        metric_types = '\n\t'.join([f'- {metric_type}' for metric_type in cls.metrics])

        return f'''
        Reporter Metrics


        Metrics are collections of aggregated statistics for actions. Each action may have many types metrics, but only 
        one of a given type of metric will/should be calculated for a given action. Metrics should contain single aggregate
        values computed off the event metric field of events (see above). Like events, metrics vary by reporter due to the
        requirements of the given resource a reporter integrates with, but contain the same general fields:

        - metric name: The name of the action associated with the metric. May also contain the metric method (i.e. GET)

        - metric value: The aggregated value of the event metric field for all events of a given action. For example, average
                        request time.
        
        - metric host: The base address of the target URI/URL the action executed against.

        - metric url: The full address of the target URI/URL the action executed against.

        - metric stat: The type of aggregate metric computed (for example - avg, min, max).

        - metric type: The type of metric - the use of this field varies by reporter, but often determines whether a metric
                       is consumed as a count/gauge/histogram/etc. by the resource specified by the submit reporter.

        - metric tage: A list of dictionaries with `tag_name` and `tag_value` as keys. Note that
                      for some reporters, the name and value of a tag are combined as a single,
                      colon-delimited string (i.e. "<tag_name>:<tag_value>"). Tags are created
                      based upon the tags specified for a given action.


        Metrics are linked to the update reporter type and generate timestamps upon submission to the resource specified
        by the submit reporter. If a metric is submitted that is not the correct type it will be automatically converted 
        to the type specified by the submit reporter. Currently supported metric types include:

        {metric_types} 
        
        For more information on exact metric type specification for a given reporter, run the command:

            hedra --about results:metrics:<reporter_type>

        NOTE: As an end user, you will never need to directly interact with Metrics, their conversion, submission,
        or handling outside of specifying which submit reporter Hedra should use.
        
        '''

    def __str__(self):
        return self.metric.__str__()

    def __repr__(self):
        return self.metric.__repr__()

    def get_naive_time(self):
        return datetime.datetime.utcnow()

    def get_utc_time(self):
        return self.time.astimezone(
            datetime.timezone.utc
        ).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        )

    def get_local_time(self):
        return self.machine_timezone.localize(
            self.time
        ).strftime(
            "%Y-%m-%dT%H:%M:%S:%z"
        )

    def to_dict(self):
        return {field: getattr(self.metric, field) for field in self.metric.fields}

    def convert(self, metric_type):
        new_metric = self.metrics.get(metric_type)
        if new_metric is None:
            new_metric = StatStreamMetric

        self.metric = new_metric(
            stat=self.metric.stat,
            value=self.metric.value,
            metadata=self.metric.metadata
        )
        self.format = self.metric.format
        self.fields = self.metric.fields
        self.value = self.metric.metric_value
        self.tags = self.metric.tags

        return self