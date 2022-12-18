import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    DogStatsDConfig
)


class SubmitDogStatsDResultsStage(Submit):
    config=DogStatsDConfig(
        host='localhost',
        port=8125
    )