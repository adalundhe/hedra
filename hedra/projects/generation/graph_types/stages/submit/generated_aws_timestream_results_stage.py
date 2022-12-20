import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    AWSTimestreamConfig
)


class SubmitAWSTimestreamResultsStage(Submit):
    config=AWSTimestreamConfig(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        region_name=os.getenv("AWS_REGION_NAME", ''),
        database_name='results',
        events_table='events',
        metrics_table='metrics'
    )