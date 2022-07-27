from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class DatadogConfig(BaseModel):
    api_key: str
    app_key: str
    event_alert_type: str='info'
    device_name: str='hedra'
    priority: str='normal'
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Datadog