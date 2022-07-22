from __future__ import annotations
import uuid
from hedra.reporting.connectors.types.redis_connector import RedisConnector as Redis
from .utils.tools.redis_tools import (
    to_redis_metric
)


class RedisReporter:

    def __init__(self, config):
        self.format = 'redis'
        self.reporter_config = config
        self.redis = None
        self.session_id = uuid.uuid4()

    @classmethod
    def about(cls):
        return '''
        Redis Reporter - (redis)

        The Redis reporter allows you to submit events and metrics to Redis for storage. Note that
        Redis is an in-memory key/value store - for more permanent storage we recommend retrieving 
        events and metrics as soon as possible and storing them in a traditional database.

        '''

    async def init(self) -> RedisReporter:    
        self.redis = Redis(self.reporter_config)
        await self.redis.connect()

    async def submit(self, metric) -> RedisReporter:
        redis_metric_insert = to_redis_metric(self.session_id, metric)
        
        await self.redis.execute(redis_metric_insert, finalize=True)
        return self

    async def close(self) -> RedisReporter:
        await self.redis.clear()
        await self.redis.close()
        return self
