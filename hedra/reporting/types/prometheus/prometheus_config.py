from typing import Any, Dict
from pydantic import BaseModel
from typing import Optional
from hedra.reporting.types.common.types import ReporterTypes


class PrometheusConfig(BaseModel):
    pushgateway_address: str='localhost:9091'
    auth_request_method: str='GET'
    auth_request_timeout: int=60000
    auth_request_data: Dict[str, Any]={}
    username: Optional[str]
    password: Optional[str]
    namespace: Optional[str]
    job_name: str='hedra'
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Prometheus