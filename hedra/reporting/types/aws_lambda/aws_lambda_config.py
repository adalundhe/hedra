from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes
from typing import Optional


class AWSLambdaConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    events_lambda: str
    metrics_lambda: str
    experiments_lambda: Optional[str]
    streams_lambda: Optional[str]
    reporter_type: ReporterTypes=ReporterTypes.AWSLambda