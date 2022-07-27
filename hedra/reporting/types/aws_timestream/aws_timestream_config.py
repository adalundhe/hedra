from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class AWSTimestreamConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    database_name: str
    events_table: str='events'
    metrics_table: str='metrics'
    retention_options: Dict[str, int] = {
        "MemoryStoreRetentionPeriodInHours": 1,
        "MagneticStoreRetentionPeriodInDays": 365,
    }
    reporter_type: ReporterTypes=ReporterTypes.AWSTimestream