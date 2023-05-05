from ssl import SSLContext
from typing import List, Optional
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class CassandraConfig(BaseModel):
    hosts: List[str] = ['127.0.0.1']
    port: int=9042
    username: Optional[str]=None
    password: Optional[str]=None
    keyspace: str='hedra'
    events_table: str='events'
    metrics_table: str='metrics'
    streams_table: str='streams'
    experiments_table: str='experiments'
    replication_strategy: str='SimpleStrategy'
    replication: int=3
    ssl: Optional[SSLContext]=None
    reporter_type: ReporterTypes=ReporterTypes.Cassandra

    class Config:
        arbitrary_types_allowed = True