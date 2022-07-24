from hedra.reporting.types.common.types import ReporterTypes


class GCSConfig:
    token: str=None
    events_bucket: str=None
    metrics_bucket: str=None
    reporter_type: ReporterTypes=ReporterTypes.GCS