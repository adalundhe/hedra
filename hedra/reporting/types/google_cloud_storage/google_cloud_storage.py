import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import re
from typing import List

import psutil
from hedra.reporting import metric
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup, timings_group

try:

    from google.cloud import storage
    from .google_cloud_storage_config import GoogleCloudStorageConfig
    has_connector = True

except Exception:
    storage = None
    GoogleCloudStorageConfig = None
    has_connector = False

class GoogleCloudStorage:

    def __init__(self, config: GoogleCloudStorageConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.bucket_namespace = config.bucket_namespace
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.errors_bucket_name = f'{self.metrics_bucket_name}_errors'

        self.credentials = None
        self.client = None
        self.events_bucket = None
        self.metrics_bucket = None
        self.errors_bucket = None

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = storage.Client.from_service_account_json(self.service_account_json_path)

    async def submit_events(self, events: List[BaseEvent]):

        try:

            self.events_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
            )
        
        except Exception:

            self.events_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
            )

        for event in events:
            blob = await self._loop.run_in_executor(
                self._executor,
                self.events_bucket.blob,
                event.name
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(event.record)
            )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        try:

            self.metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
            )
        
        except Exception:

            self.metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
            )

        for metrics_group in metrics:

            for timings_group_name, timings_group in metrics_group.groups.items():
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self.metrics_bucket.blob,
                    metrics_group.name
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        **timings_group.record,
                        'timings_group': timings_group_name
                    })
                )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        try:

            self.errors_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.errors_bucket_name}'
            )
        
        except Exception:

            self.errors_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.errors_bucket_name}'
            )

        for metrics_group in metrics_groups:
            for error in metrics_group.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self.metrics_bucket.blob,
                    f'{metrics_group.name}_{error_message}'
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        'metric_name': metrics_group.name,
                        'metric_stage': metrics_group.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })
                )

            

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.client.close
        )