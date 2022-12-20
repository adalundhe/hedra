from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    NetdataConfig
)


class SubmitNetdataResultsStage(Submit):
    config=NetdataConfig(
        host='localhost',
        port=8125
    )