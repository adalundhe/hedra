import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    MongoDBConfig
)


class SubmitMongoDBResultsStage(Submit):
    config=MongoDBConfig(
        host='localhost:27017',
        username=os.getenv('MONGODB_USERNAME', ''),
        password=os.getenv('MONGODB_PASSWORD', ''),
        database='results',
        events_collection='events',
        metrics_collection='metrics'
    )