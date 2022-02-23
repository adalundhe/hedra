from __future__ import annotations
import uuid
from hedra.connectors.types.kafka_connector import KafkaConnector as Kafka


class KafkaReporter:

    def __init__(self, config):
        self.format = 'kafka'
        self.session_id = uuid.uuid4()
        self.reporter_config = config

        if len(self.reporter_config) < 1:
            self.reporter_config = {
                'kafka_serializer': {
                    'kafka_serializer_type': 'json'
                }
            }


        self._session_events_topic = 'hedra_events'
        self._session_metrics_topic = 'hedra_metrics'

        kafka_topics = self.reporter_config.get('message_topics')

        if kafka_topics and len(kafka_topics) > 0:
            self._session_events_topic = kafka_topics.get('events_topic', 'hedra_events')
            self._session_metrics_topic = kafka_topics.get('metrics_topic', 'hedra_metrics')

        self.reporter_config['kafka_topics'] = [
            self._session_events_topic,
            self._session_metrics_topic
        ]

        self.connector = Kafka(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        Kafka Reporter - (kafka)

        The Kafka reporter allows you to submit event/metrics as messages to via a Kafka Producer to specified
        Kafka event/metric topics and to retrieve aggregated results via Kafka Consuper. If no topics are specified, 
        defaults of "hedra_events" will be used for events and "hedra_metrics" will be used for metrics.
        
        '''

    async def init(self) -> KafkaReporter:
        await self.connector.connect()
        return self
    
    async def update(self, event) -> list:
        await self.connector.execute({
            'type': 'update',
            'message_topic': self._session_events_topic
            **event.event.to_format()
        })

        return await self.connector.commit()

    async def merge(self, connector) -> KafkaReporter:
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        await self.connector.execute({
            'type': 'query',
            'message_topic': self._session_events_topic
        })

        return await self.connector.commit()

    async def submit(self, metric) -> KafkaReporter:
        await self.connector.execute({
            'type': 'update',
            'message_topic': self._session_metrics_topic,
            **metric.metric.to_format()
        })

        return await self.connector.commit()

    async def close(self) -> KafkaReporter:
        await self.connector.clear()
        await self.connector.close()
        return self

    
