from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    GraphiteConfig
)


class SubmitGraphiteResultsStage(Submit):
    config=GraphiteConfig(
        host='localhost',
        port=2003
    )