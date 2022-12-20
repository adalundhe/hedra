import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    NewRelicConfig
)


class SubmitNewrelicResultsStage(Submit):
    config=NewRelicConfig(
        config_path=os.getenv('NEWRELIC_CONFIG_PATH', ''),
        environment=os.getenv('NEWRELIC_ENVIRONMENT', ''),
        newrelic_application_name='results'
    )