from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    StatsDConfig
)


class SubmitStatsDResultsStage(Submit):
    config=StatsDConfig(
        host='localhost',
        port=8125
    )