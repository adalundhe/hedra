import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    TimescaleDBConfig
)


class SubmitTimescaleDBResultsStage(Submit):
    config=TimescaleDBConfig(
        host='127.0.0,1',
        database='results',
        username=os.getenv('TIMESCALEDB_USERNAME', ''),
        password=os.getenv('TIMESCALEDB_PASSWORD', ''),
        events_table='events',
        metrics_table='metrics'
    )