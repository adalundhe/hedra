import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    BigTableConfig
)


class SubmitBigTableResultsStage(Submit):
    config=BigTableConfig(
        service_account_json_path=os.getenv('GOOGLE_CLOUD_ACCOUNT_JSON_PATH', ''),
        instance_id=os.getenv('GOOGLE_CLOUD_BIGTABLE_INSTANCE_ID', ''),
        events_table='events',
        metrics_table='metrics'
    )