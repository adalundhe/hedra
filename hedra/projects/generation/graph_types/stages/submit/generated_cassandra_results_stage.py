import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    CassandraConfig
)


class SubmitCassandraResultsStage(Submit):
    config=CassandraConfig(
        hosts=['127.0.0.1'],
        port=9042,
        username=os.getenv('CASSANDRA_USERNAME', ''),
        password=os.getenv('CASSANDRA_PASSWORD', ''),
        keyspace='results',
        events_table='events',
        metrics_table='metrics'
    )