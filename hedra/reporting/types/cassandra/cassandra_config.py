from typing import Dict, List
from hedra.reporting.types.common.types import ReporterTypes


class CassandraConfig:
    hosts: List[str] = []
    port: int=None
    username: str=None
    password: str=None
    keyspace: str=None
    custom_fields: Dict[str, str]={}
    events_table: str=None
    metrics_table: str=None
    replication_strategy: str='SimpleStrategy'
    replication: int=3
    reporter_type: ReporterTypes=ReporterTypes.Cassandra
