from ssl import SSLContext
from types import SimpleNamespace
from typing import Dict, List, Optional
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes

try:

    from cassandra.cqlengine import columns

except Exception:
    columns = SimpleNamespace(Column=None)


class CassandraConfig(BaseModel):
    hosts: List[str] = ['127.0.0.1']
    port: int=9042
    username: Optional[str]=None
    password: Optional[str]=None
    keyspace: str='hedra'
    custom_fields: Dict[str, columns.Column]={}
    events_table: str='events'
    metrics_table: str='metrics'
    replication_strategy: str='SimpleStrategy'
    replication: int=3
    ssl: Optional[SSLContext]=None
    reporter_type: ReporterTypes=ReporterTypes.Cassandra

    class Config:
        arbitrary_types_allowed = True