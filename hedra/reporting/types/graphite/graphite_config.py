from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class GraphiteConfig(BaseModel):
    host: str='localhost'
    port: int=2003
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Graphite