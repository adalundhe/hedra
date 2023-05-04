import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class CSVConfig(BaseModel):
    events_filepath: str=os.path.join(
        os.getcwd(),
        'events.csv'
    )
    metrics_filepath: str=os.path.join(
        os.getcwd(),
        'metrics.csv'
    )
    experiments_filepath: str=os.path.join(
        os.getcwd(),
        'experiments.csv'
    )
    streams_filepath: str=os.path.join(
        os.getcwd(),
        'streams.csv'
    )
    overwrite: bool=True
    reporter_type: ReporterTypes=ReporterTypes.CSV