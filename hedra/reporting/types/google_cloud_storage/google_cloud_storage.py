import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import re
from typing import List

import psutil
from hedra.reporting import metric
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

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
        self.group_metrics_bucket_name = f'{self.group_metrics_bucket_name}_group_metrics'
        self.errors_bucket_name = f'{self.metrics_bucket_name}_errors'

        self.credentials = None
        self.client = None
        self.events_bucket = None
        self.group_metrics_bucket = None
        self.metrics_bucket = None
        self.errors_bucket = None
        self._custom_metrics_buckets = {}

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

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        try:

            self.group_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.group_metrics_bucket_name}'
            )
        
        except Exception:

            self.group_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.group_metrics_bucket_name}'
            )

        for metrics_set in metrics_sets:

            blob = await self._loop.run_in_executor(
                self._executor,
                self.metrics_bucket.blob,
                metrics_set.name
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'common',
                    **metrics_set.common_stats
                })
            )

    async def submit_metrics(self, metrics: List[MetricsSet]):

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

        for metrics_set in metrics:

            for group_name, group in metrics_set.groups.items():
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self.metrics_bucket.blob,
                    metrics_set.name
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        **group.record,
                        'group': group_name
                    })
                )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_bucket_name = f'{self.bucket_namespace}_{self.metrics_bucket_name}_{custom_group_name}_metrics'

                try:

                    self._custom_metrics_buckets[custom_bucket_name] = await self._loop.run_in_executor(
                        self._executor,
                        self.client.get_bucket,
                        custom_bucket_name
                    )
                
                except Exception:

                    self._custom_metrics_buckets[custom_bucket_name] = await self._loop.run_in_executor(
                        self._executor,
                        self.client.create_bucket,
                        custom_bucket_name
                    )

                blob = await self._loop.run_in_executor(
                    self._executor,
                    self.metrics_bucket.blob,
                    f'{metrics_set.name}_{custom_bucket_name}'
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': custom_group_name,
                        **group
                    })
                )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

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

        for metrics_set in metrics_sets:
            for error in metrics_set.errors:
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
                    f'{metrics_set.name}_{error_message}'
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        'metric_name': metrics_set.name,
                        'metric_stage': metrics_set.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })
                )

            

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.client.close
        )