from hedra.reporting.types.common.types import ReporterTypes


class CosmosDBConfig:
    account_uri: str=None
    account_key: str=None
    database: str=None
    events_container: str=None
    metrics_container: str=None
    analytics_ttl: int=0
    events_partition: str=None
    metrics_partition: str=None
    reporter_type: ReporterTypes=ReporterTypes.CosmosDB