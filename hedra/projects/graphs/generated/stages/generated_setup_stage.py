from hedra.core.graphs.stages import (
    Setup
)


class SetupStage(Setup):
    batch_size=1000
    total_time='1m'