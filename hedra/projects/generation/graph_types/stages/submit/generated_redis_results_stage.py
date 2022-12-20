import os
from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    RedisConfig
)


class SubmitRedisResultsStage(Submit):
    config=RedisConfig(
        host='localhost:6379',
        username=os.getenv('REDIS_USERNAME', ''),
        password=os.getenv('REDIS_PASSWORD', ''),
        events_channel='events',
        metrics_channel='metrics',
        channel_type='pipeline',
        secure=True
    )