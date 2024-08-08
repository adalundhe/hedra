import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    AWSLambdaConfig
)


class SubmitAWSLambdaResultsStage(Submit):
    config=AWSLambdaConfig(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        region_name=os.getenv("AWS_REGION_NAME", ''),
        events_lambda='events',
        metrics_lambda='metrics',
        experiments_lambda='experiments',
        streams_lambda='streams'
    )