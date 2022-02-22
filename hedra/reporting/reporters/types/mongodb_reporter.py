from __future__ import annotations
import uuid
from zebra_automate_connect.types.mongodb_connector import MongoDBConnector as MongoDB
from .utils.tools.mongodb_tools import (
    to_record,
    to_query
)


class MongoDBReporter:

    def __init__(self, config):
        self.format = 'mongodb'
        self.session_id = uuid.uuid4()
        self.reporter_config = config

        if len(self.reporter_config) < 1:
            self.reporter_config = {
                'database': 'session_{session_id}'.format(
                    session_id=self.session_id
                )
            }

        self.connector = MongoDB(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        MongoDB Reporter - (mongodb)

        The MongoDB reporter allows you to store events/metrics via MongoDB collections. If no
        colletion names are specified, events will be stored in the "hedra_events" collection and
        metrics will be stored in the "hedra_metrics" collection.

        '''

    async def init(self) -> MongoDBReporter:
        self.events_collection = self.reporter_config.get('events', 'hedra_events')
        self.metrics_collection = self.reporter_config.get('metrics', 'hedra_metrics')
        await self.connector.connect()
        return self

    async def update(self, event) -> list:
        insert_event_query = to_record(
            collection_name=self.events_collection,
            data=event
        )
       
        await  self.connector.execute(insert_event_query)
        return await self.connector.commit()

    async def merge(self, connector) -> MongoDBReporter:
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        events_fetch_query = to_query(
            collection_name=self.events_collection,
            key=key,
            stat_field=stat_field,
            stat_type=stat_type,
            partial=partial
        )

        await self.connector.execute(
            events_fetch_query
        )
        
        return await self.connector.commit()
    
    async def submit(self, metric) -> MongoDBReporter:
        insert_metric_query = to_record(
            collection_name=self.metrics_collection,
            data=metric
        )
        await self.connector.execute(insert_metric_query)
        return self

    async def close(self) -> MongoDBReporter:
        await self.connector.close()
        return self