import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class CSVConfig(BaseModel):
    events_filepath: str=f'{os.getcwd()}/events.csv'
    metrics_filepath: str=f'{os.getcwd()}/metrics.csv'
    reporter_type: ReporterTypes=ReporterTypes.CSV