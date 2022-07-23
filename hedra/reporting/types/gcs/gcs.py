import asyncio
import json
from typing import Any, List

try:

    from google.cloud import storage
    from google.auth.credentials import Credentials
    has_connector = True

except ImportError:
    has_connector = False

class GCS:

    def __init__(self, config: Any) -> None:
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

    async def submit_events(self, events: List[Any]):

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

    async def submit_metrics(self, metrics: List[Any]):

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