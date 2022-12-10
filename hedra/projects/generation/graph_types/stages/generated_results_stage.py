from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    JSONConfig
)


class SubmitResultsStage(Submit):
    config=JSONConfig(
        events_filepath='./events.json',
        metrics_filepath='./metrics.json'
    )