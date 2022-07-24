from typing import Any, Dict

from hedra.reporting.types.common.types import ReporterTypes


class PrometheusConfig:
    pushgateway_address: str=None
    auth_request_method: str='GET'
    auth_request_timeout: int=60000
    auth_request_data: Dict[str, Any]={}
    username: str=None
    password: str=None
    namespace: str=None
    job_name: str=None
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Prometheus