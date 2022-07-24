from hedra.reporting.types.common.types import ReporterTypes


class TelegrafConfig:
    host: str=None
    port: str=None
    reporter_type: ReporterTypes=ReporterTypes.Telegraf