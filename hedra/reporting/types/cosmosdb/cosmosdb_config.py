from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class CosmosDBConfig(BaseModel):
    account_uri: str
    account_key: str
    database: str
    events_container: str='events'
    metrics_container: str='metrics'
    events_partition_key: str='name'
    metrics_partition_key: str='name'
    analytics_ttl: int=0
    reporter_type: ReporterTypes=ReporterTypes.CosmosDB