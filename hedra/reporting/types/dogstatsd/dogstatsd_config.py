from typing import Dict
from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel


class DogStatsDConfig(BaseModel):
    host: str='localhost'
    port: int=8125
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.DogStatsD