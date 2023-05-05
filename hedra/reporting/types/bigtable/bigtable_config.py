from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class BigTableConfig(BaseModel):
    service_account_json_path: str
    instance_id: str
    events_table: str = 'events'
    metrics_table: str = 'metrics'
    experiments_table: str= 'experiments'
    streams_table: str='streams'
    reporter_type: ReporterTypes=ReporterTypes.BigTable

