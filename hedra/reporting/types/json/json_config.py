import os
from hedra.reporting.types.common.types import ReporterTypes


class JSONConfig:
    events_filepath: str=f'{os.getcwd()}/events.json'
    metrics_filepath: str=f'{os.getcwd()}/metrics.json'
    reporter_type: ReporterTypes=ReporterTypes.JSON