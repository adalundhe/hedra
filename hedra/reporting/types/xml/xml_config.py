import os
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class XMLConfig(BaseModel):
    events_filepath: str=os.path.join(
        os.getcwd(),
        'events.xml'
    )
    metrics_filepath: str=os.path.join(
        os.getcwd(),
        'metrics.xml'
    )
    experiments_filepath: str=os.path.join(
        os.getcwd(),
        'experiments.xml'
    )
    overwrite: bool=True
    reporter_type: ReporterTypes=ReporterTypes.XML
