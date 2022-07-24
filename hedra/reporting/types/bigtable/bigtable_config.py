from hedra.reporting.types.common.types import ReporterTypes


class BigTableConfig:
    token: str = None
    instance: str = None
    events_table: str = None
    metrics_table: str = None
    reporter_type: ReporterTypes=ReporterTypes.BigTable

