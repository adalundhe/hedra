from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes
from typing import Optional


class AWSLambdaConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    events_lambda: str='hedra_events'
    metrics_lambda: str='hedra_metrics'
    system_metrics_lambda: str='hedra_system_metrics'
    experiments_lambda: Optional[str]
    streams_lambda: Optional[str]
    reporter_type: ReporterTypes=ReporterTypes.AWSLambda