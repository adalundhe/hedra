from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes


class TelegrafStatsDConfig:
    host: str=None
    port: str=None
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.TelegrafStatsD