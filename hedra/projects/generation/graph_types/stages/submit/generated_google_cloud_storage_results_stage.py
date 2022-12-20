import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    GoogleCloudStorageConfig
)


class SubmitGoogleCloudStorageResultsStage(Submit):
    config=GoogleCloudStorageConfig(
        service_account_json_path=os.getenv('GOOGLE_CLOUD_ACCOUNT_JSON_PATH', ''),
        bucket_namespace='results',
        events_bucket='events',
        metrics_bucket='metrics'
    )