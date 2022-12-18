from typing import List
from pydantic import BaseModel, conlist
from hedra.reporting.types.common.types import ReporterTypes


class _CloudwatchTarget(BaseModel):
    arn: str
    id: str

class CloudwatchConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    iam_role_arn: str
    schedule_rate: str=None
    events_rule: str='hedra-events'
    metrics_rule: str='hedra-metrics'
    cloudwatch_targets: conlist(_CloudwatchTarget, min_items=1)
    aws_resource_arns: List[str]=[]
    cloudwatch_source: str='hedra'
    submit_timeout: int=60
    reporter_type: ReporterTypes=ReporterTypes.Cloudwatch