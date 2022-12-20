import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    DatadogConfig
)


class SubmitDatadogResultsStage(Submit):
    config=DatadogConfig(
        api_key=os.getenv('DATADOG_API_KEY', ''),
        app_key=os.getenv('DATADOG_APP_KEY', '')
    )