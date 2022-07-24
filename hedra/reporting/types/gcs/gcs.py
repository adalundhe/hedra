import asyncio
import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:

    from google.cloud import storage
    from google.auth.credentials import Credentials
    from .gcs_config import GCSConfig
    has_connector = True

except ImportError:
    storage = None
    Credentials = None
    GCSConfig = None
    has_connector = False

class GCS:

    def __init__(self, config: GCSConfig) -> None:
        self.token = config.token
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.credentials = Credentials()
        self.credentials.token = self.token

        self.client = storage.Client(credentials=self.credentials)

    async def submit_events(self, events: List[BaseEvent]):

        events_bucket = None

        try:

            events_bucket = await self._loop.run_in_executor(
                None,
                self.client.get_bucket,
                self.events_bucket_name
            )
        
        except Exception:

            events_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                self.events_bucket_name
            )

        for event in events:
            blob = await self._loop.run_in_executor(
                None,
                events_bucket.blob,
                event.name
            )

            await self._loop.run_in_executor(
                None,
                blob.upload_from_string,
                json.dumps(event.record)
            )

    async def submit_metrics(self, metrics: List[Metric]):

        metrics_bucket = None

        try:

            metrics_bucket = await self._loop.run_in_executor(
                None,
                self.client.get_bucket,
                self.metrics_bucket_name
            )
        
        except Exception:

            metrics_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                self.metrics_bucket_name
            )

        for metric in metrics:
            blob = await self._loop.run_in_executor(
                None,
                metrics_bucket.blob,
                metric.name
            )

            await self._loop.run_in_executor(
                None,
                blob.upload_from_string,
                json.dumps(metric.record)
            )

    async def close(self):
        pass