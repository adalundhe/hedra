from hedra.reporting.types.common.types import ReporterTypes


class DogStatsDConfig:
    host: str=None
    post: int=None
    reporter_type: ReporterTypes=ReporterTypes.DogStatsD