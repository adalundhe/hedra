from hedra.reporting.types.common.types import ReporterTypes


class NewRelicConfig:
    registration_timeout: int=60
    shutdown_timeout: int=60
    newrelic_application_name: str='hedra'
    reporter_type: ReporterTypes=ReporterTypes.NewRelic