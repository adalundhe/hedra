from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes


class DatadogConfig:
    api_key: str=None
    app_key: str=None
    event_alert_type: str='info'
    device_name: str='hedra'
    priority: str='normal'
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Datadog