import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class JSONConfig(BaseModel):
    events_filepath: str=os.path.join(
        os.getcwd(),
        'events.json'
    )
    metrics_filepath: str=os.path.join(
        os.getcwd(),
        'metrics.json'
    )
    experiments_filepath: str=os.path.join(
        os.getcwd(),
        'experiments.json'
    )
    streams_filepath: str=os.path.join(
        os.getcwd(),
        'streams.json'
    )
    overwrite: bool=True
    reporter_type: ReporterTypes=ReporterTypes.JSON