import asyncio
import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:

    from google.cloud import storage
    from .google_cloud_storage_config import GoogleCloudStorageConfig
    has_connector = True

except ImportError:
    storage = None
    GoogleCloudStorageConfig = None
    has_connector = False

class GoogleCloudStorage:

    def __init__(self, config: GoogleCloudStorageConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.bucket_namespace = config.bucket_namespace
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = storage.Client.from_service_account_json(self.service_account_json_path)

    async def submit_events(self, events: List[BaseEvent]):

        events_bucket = None

        try:

            events_bucket = await self._loop.run_in_executor(
                None,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
            )
        
        except Exception:

            events_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
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
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
            )
        
        except Exception:

            metrics_bucket = await self._loop.run_in_executor(
                None,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
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
        await self._loop.run_in_executor(
            None,
            self.client.close
        )