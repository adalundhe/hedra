from hedra.reporting.types.common.types import ReporterTypes


class GraphiteConfig:
    host: str=None
    port: int=None
    reporter_type: ReporterTypes=ReporterTypes.Graphite