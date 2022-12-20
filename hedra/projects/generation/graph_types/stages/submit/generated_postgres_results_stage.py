import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    PostgresConfig
)


class SubmitPostgresResultsStage(Submit):
    config=PostgresConfig(
        host='127.0.0,1',
        database='results',
        username=os.getenv('POSTGRES_USERNAME', ''),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        events_table='events',
        metrics_table='metrics'
    )