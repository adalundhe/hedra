from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    JSONConfig
)


class SubmitJSONResultsStage(Submit):
    config=JSONConfig(
        events_filepath='./events.json',
        metrics_filepath='./metrics.json'
    )