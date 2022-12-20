import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    S3Config
)


class SubmitS3ResultsStage(Submit):
    config=S3Config(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        region_name=os.getenv('AWS_REGION_NAME', ''),
        buckets_namespace='results',
        events_bucket='events',
        metrics_bucket='metrics'
    )