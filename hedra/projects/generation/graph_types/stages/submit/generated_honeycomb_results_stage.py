import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    HoneycombConfig
)


class SubmitHoneycombResultsStage(Submit):
    config=HoneycombConfig(
        api_key=os.getenv('HONEYCOMB_API_KEY', ''),
        dataset='results'
    )