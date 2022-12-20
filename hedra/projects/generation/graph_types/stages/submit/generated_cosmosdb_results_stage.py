import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    CosmosDBConfig
)


class SubmitCosmosDBResultsStage(Submit):
    config=CosmosDBConfig(
        account_uri=os.getenv('AZURE_COSMOSDB_ACCOUNT_URI', ''),
        account_key=os.getenv('AZURE_COSMOSDB_ACCOUNT_KEY', ''),
        database='results',
        events_container='events',
        metrics_container='metrics',
        events_partition_key='name',
        metrics_partition_key='name'
    )