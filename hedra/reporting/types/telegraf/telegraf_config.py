from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class TelegrafConfig(BaseModel):
    host: str='localhost'
    port: str=8094
    reporter_type: ReporterTypes=ReporterTypes.Telegraf