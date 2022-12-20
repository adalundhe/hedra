import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    BigQueryConfig
)


class SubmitBigQueryResultsStage(Submit):
    config=BigQueryConfig(
        service_account_json_path=os.getenv('GOOGLE_CLOUD_ACCOUNT_JSON_PATH', ''),
        project_name='test',
        dataset_name='results',
        events_table='events',
        metrics_table='metrics'
    )