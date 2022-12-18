from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    CSVConfig
)


class SubmitCSVResultsStage(Submit):
    config=CSVConfig(
        events_filepath='./events.json',
        metrics_filepath='./metrics.json'
    )