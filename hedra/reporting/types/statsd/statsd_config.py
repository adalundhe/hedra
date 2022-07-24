from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes


class StatsDConfig:
    host: str=None
    port: int=None
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.StatsD