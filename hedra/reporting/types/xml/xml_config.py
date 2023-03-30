import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class XMLConfig(BaseModel):
    events_filepath: str=f'{os.getcwd()}/events.xml'
    metrics_filepath: str=f'{os.getcwd()}/metrics.xml'
    reporter_type: ReporterTypes=ReporterTypes.XML
