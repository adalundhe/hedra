from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes


class NetdataConfig:
    host: str=None
    post: int=None
    custom_fields: Dict[str, str]={}
    reporter_type: ReporterTypes=ReporterTypes.Netdata