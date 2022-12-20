import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    SQLiteConfig
)


class SubmitSQLiteResultsStage(Submit):
    config=SQLiteConfig(
        path=f'{os.getcwd}/results.db',
        events_table='events',
        metrics_table='metrics'
    )