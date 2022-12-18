from hedra.core.graphs.stages import (
    Submit
)

from hedra.reporting.types import (
    KafkaConfig
)


class SubmitKafkaResultsStage(Submit):
    config=KafkaConfig(
        host='localhost:9092',
        client_id='results',
        events_topic='events',
        metrics_topic='metrics'
    )