from typing import Optional
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class NewRelicConfig(BaseModel):
    config_path: str
    environment: Optional[str]=None
    registration_timeout: int=60
    shutdown_timeout: int=60
    newrelic_application_name: str='hedra'
    reporter_type: ReporterTypes=ReporterTypes.NewRelic