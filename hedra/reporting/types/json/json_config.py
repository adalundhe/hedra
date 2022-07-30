import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class JSONConfig(BaseModel):
    events_filepath: str=f'{os.getcwd()}/events.json'
    metrics_filepath: str=f'{os.getcwd()}/metrics.json'
    reporter_type: ReporterTypes=ReporterTypes.JSON