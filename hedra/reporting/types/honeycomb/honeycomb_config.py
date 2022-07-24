from hedra.reporting.types.common.types import ReporterTypes


class HoneycombConfig:
    api_key: str=None
    dataset: str=None
    reporter_type: ReporterTypes=ReporterTypes.Honeycomb