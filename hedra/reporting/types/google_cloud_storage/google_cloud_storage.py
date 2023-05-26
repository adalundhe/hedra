import asyncio
import json
import psutil
import uuid
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.system.system_metrics_set import (
    SystemMetricsSet,
    SessionMetricsCollection,
    SystemMetricsCollection
)
from .google_cloud_storage_config import GoogleCloudStorageConfig

try:

    from google.cloud import storage
    has_connector = True

except Exception:
    storage = None
    has_connector = False

class GoogleCloudStorage:

    def __init__(self, config: GoogleCloudStorageConfig) -> None:
        self.service_account_json_path = config.service_account_json_path

        self.bucket_namespace = config.bucket_namespace
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.streams_bucket_name = config.streams_bucket

        self.experiments_bucket_name = config.experiments_bucket
        self.variants_bucket_name = f'{config.experiments_bucket}_variants'
        self.mutations_bucket_name = f'{config.experiments_bucket}_mutations'
        
        self.shared_metrics_bucket_name = f'{config.metrics_bucket}_shared'
        self.errors_bucket_name = f'{config.metrics_bucket}_errors'
        self.custom_metrics_bucket_name = f'{config.metrics_bucket}_custom'

        self.session_system_metrics_bucket_name = f'{config.system_metrics_bucket}_session'
        self.stage_system_metrics_bucket_name = f'{config.system_metrics_bucket}_stage'

        self.credentials = None
        self.client = None

        self._events_bucket = None
        self._streams_bucket = None

        self._experiments_bucket = None
        self._variants_bucket = None
        self._mutations_bucket = None

        self._shared_metrics_bucket = None
        self._metrics_bucket = None
        self._errors_bucket = None
        self._custom_metrics_bucket = None
        self._session_system_metrics_bucket = None
        self._stage_system_metrics_bucket = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to Google Cloud - Loading account config from - {self.service_account_json_path}')
        self.client = storage.Client.from_service_account_json(self.service_account_json_path)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Loaded account config from - {self.service_account_json_path}')

    async def submit_session_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Session System Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.session_system_metrics_bucket_name} if not exists')

            self._session_system_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.session_system_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Session System Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.session_system_metrics_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Session System Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.session_system_metrics_bucket_name}')

            self._session_system_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.session_system_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Session System Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.session_system_metrics_bucket_name}')

        metrics_sets: List[SessionMetricsCollection] = []
        
        for metrics_set in system_metrics_sets:
            for monitor_metrics in metrics_set.session_cpu_metrics.values():
                metrics_sets.append(monitor_metrics)
                
            for  monitor_metrics in metrics_set.session_memory_metrics.values():
                metrics_sets.append(monitor_metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Session System Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.session_system_metrics_bucket_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics Set - {metrics_set.name}:{metrics_set.group}')

            blob = await self._loop.run_in_executor(
                self._executor,
                self._streams_bucket.blob,
                f'{metrics_set.name}_{metrics_set.group}_{self.session_uuid}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(metrics_set.record)
            )
    
    async def submit_stage_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Stage System Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.stage_system_metrics_bucket_name} if not exists')

            self._stage_system_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.stage_system_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Stage System Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.stage_system_metrics_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Stage System Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.stage_system_metrics_bucket_name}')

            self._stage_system_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.stage_system_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Stage System Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.stage_system_metrics_bucket_name}')

        metrics_sets: List[SystemMetricsCollection] = []
        
        for metrics_set in system_metrics_sets:
            cpu_metrics = metrics_set.cpu
            memory_metrics = metrics_set.memory

            for stage_name, stage_cpu_metrics in  cpu_metrics.metrics.items():

                for monitor_metrics in stage_cpu_metrics.values():
                    metrics_sets.append(monitor_metrics)

                stage_memory_metrics = memory_metrics.metrics.get(stage_name)
                for monitor_metrics in stage_memory_metrics.values():
                    metrics_sets.append(monitor_metrics)

                stage_mb_per_vu_metrics = metrics_set.mb_per_vu.get(stage_name)
                
                if stage_mb_per_vu_metrics:
                    metrics_sets.append(stage_mb_per_vu_metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Stage System Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.stage_system_metrics_bucket_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stage System Metrics Set - {metrics_set.name}:{metrics_set.group}')

            blob = await self._loop.run_in_executor(
                self._executor,
                self._streams_bucket.blob,
                f'{metrics_set.name}_{metrics_set.group}_{self.session_uuid}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(metrics_set.record)
            )

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Streams bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.streams_bucket_name} if not exists')

            self._streams_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.streams_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Streams bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.streams_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Streams bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.streams_bucket_name}')

            self._streams_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.streams_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Streams bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.streams_bucket_name}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to - Namespace: {self.bucket_namespace} - Bucket: {self.streams_bucket_name}')
        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self._streams_bucket.blob,
                    f'{stage_name}_{group_name}_{self.session_uuid}'
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        'name': f'{stage_name}_streams',
                        'stage': stage_name,
                        'group': group_name,
                        **group
                    })
                )

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Experiments bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name} if not exists')

            self._experiments_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.experiments_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Experiments bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Experiments bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name}')

            self._experiments_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.experiments_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Experiments bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name}')
        for experiment in experiment_metrics.experiment_summaries:

            experiment_id = uuid.uuid4()

            blob = await self._loop.run_in_executor(
                self._executor,
                self._events_bucket.blob,
                f'{experiment.experiment_name}_{self.session_uuid}_{experiment_id}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(experiment.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to - Namespace: {self.bucket_namespace} - Bucket: {self.experiments_bucket_name}')
    
    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Variants bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name} if not exists')

            self._variants_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.variants_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Variants bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Variants bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name}')

            self._variants_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.variants_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Variants bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name}')
        for variant in experiment_metrics.variant_summaries:

            variant_id = uuid.uuid4()

            blob = await self._loop.run_in_executor(
                self._executor,
                self._events_bucket.blob,
                f'{variant.variant_name}_{self.session_uuid}_{variant_id}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(variant.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to - Namespace: {self.bucket_namespace} - Bucket: {self.variants_bucket_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Mutations bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name} if not exists')

            self._mutations_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.mutations_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Mutations bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Mutations bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name}')

            self._mutations_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.mutations_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Mutations bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name}')
        for mutation in experiment_metrics.mutation_summaries:

            mutation_id = uuid.uuid4()

            blob = await self._loop.run_in_executor(
                self._executor,
                self._events_bucket.blob,
                f'{mutation.mutation_name}_{self.session_uuid}_{mutation_id}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(mutation.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to - Namespace: {self.bucket_namespace} - Bucket: {self.mutations_bucket_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Events bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name} if not exists')

            self._events_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Events bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Events bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name}')

            self._events_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.events_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Events bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name}')
        for event in events:
            blob = await self._loop.run_in_executor(
                self._executor,
                self._events_bucket.blob,
                f'{event.name}_{self.session_uuid}_{event.event_id}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps(event.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to - Namespace: {self.bucket_namespace} - Bucket: {self.events_bucket_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Shared Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name} if not exists')

            self._shared_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.shared_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Shared Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Shared Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name}')

            self._shared_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.shared_metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Shared Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            blob = await self._loop.run_in_executor(
                self._executor,
                self.metrics_bucket.blob,
                f'{metrics_set.name}_shared_{self.session_uuid}'
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.shared_metrics_bucket_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name} if not exists')

            self._metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name}')

            self._metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_{self.metrics_bucket_name}'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name}')
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self._metrics_bucket.blob,
                    f'{metrics_set.name}_{group_name}_{self.session_uuid}'
                )

                await self._loop.run_in_executor(
                    self._executor,
                    blob.upload_from_string,
                    json.dumps({
                        **group.record,
                        'group': group_name
                    })
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.metrics_bucket_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Custom Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name} if not exists')

            self._custom_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                self.custom_metrics_bucket_name
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Custom Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Custom Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name}')

            self._custom_metrics_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                self.custom_metrics_bucket_name
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Custom Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name}')
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            blob = await self._loop.run_in_executor(
                self._executor,
                self.metrics_bucket.blob,
                f'{metrics_set.name}_custom_{self.session_uuid}'
            )

            await self._loop.run_in_executor(
                self._executor,
                blob.upload_from_string,
                json.dumps({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    **{
                        custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    }
                })
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.custom_metrics_bucket_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Errors Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name} if not exists')

            self._errors_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.get_bucket,
                f'{self.bucket_namespace}_errors'
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Errors Metrics bucket at - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name}')
        
        except Exception:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Setting Error Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name}')

            self._errors_bucket = await self._loop.run_in_executor(
                self._executor,
                self.client.create_bucket,
                f'{self.bucket_namespace}_errors'
            )
            
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Set Errors Metrics bucket as - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                
                blob = await self._loop.run_in_executor(
                    self._executor,
                    self.metrics_bucket.blob,
                    f'{metrics_set.name}_errors_{self.session_uuid}'
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics to - Namespace: {self.bucket_namespace} - Bucket: {self.errors_bucket_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing Google Cloud connection')
        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )

        self._executor.shutdown(wait=False, cancel_futures=True)

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed Google Cloud connection')