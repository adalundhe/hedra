from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class CosmosDBConfig(BaseModel):
    account_uri: str
    account_key: str
    database: str
    events_container: str='events'
    metrics_container: str='metrics'
    streams_container: str='streams'
    experiments_container: str='experiments'
    events_partition_key: str='name'
    metrics_partition_key: str='name'
    streams_partition_key: str='name'
    experiments_partition_key: str='experiment_name'
    variants_partition_key: str='variant_name'
    mutations_partition_key: str='mutation_name'
    analytics_ttl: int=0
    reporter_type: ReporterTypes=ReporterTypes.CosmosDB