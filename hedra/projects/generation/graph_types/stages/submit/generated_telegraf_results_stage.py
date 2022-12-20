from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    TelegrafConfig
)


class SubmitTelegrafResultsStage(Submit):
    config=TelegrafConfig(
        host='localhost',
        port=8094
    )