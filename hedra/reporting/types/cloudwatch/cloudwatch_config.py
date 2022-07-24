from typing import List
from hedra.reporting.types.common.types import ReporterTypes


class CloudwatchConfig:
    aws_access_key_id: str=None
    aws_secret_access_key: str=None
    region_name: str=None
    iam_role: str=None
    schedule_rate: str=None
    cloudwatch_targets: List[str]=[]
    aws_resource_arns: List[str]=[]
    cloudwatch_source: str='hedra'
    reporter_type: ReporterTypes=ReporterTypes.Cloudwatch