from hedra.reporting.types.common.types import ReporterTypes


class CSVConfig:
    events_filepath: str=None
    metrics_filepath: str=None
    reporter_type: ReporterTypes=ReporterTypes.CSV