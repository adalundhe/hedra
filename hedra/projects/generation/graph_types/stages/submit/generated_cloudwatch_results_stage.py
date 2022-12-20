import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    CloudwatchConfig
)


class SubmitCloudwatchResultsStage(Submit):
    config=CloudwatchConfig(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        region_name=os.getenv("AWS_REGION_NAME", ''),
        iam_role_arn=os.getenv('AWS_IAM_ROLE_ARN', ''),
        events_rule='events',
        metrics_rule='metrics',
        cloudwatch_source='<CLOUDWATCH_SOURCE>',
        cloudwatch_targets=[{
            'id': '<TARGET_ID>',
            'arn': '<TARGET_ARN>'
        }],
        aws_resource_arns=['<AWS_RESOURCE_ARN>']
    )