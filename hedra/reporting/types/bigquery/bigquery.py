import asyncio
import functools
import psutil
import uuid
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import (
    MetricsSet,
    MetricType
)
from .bigquery_config import BigQueryConfig

try:
    from google.cloud import bigquery
    has_connector = True

except Exception:
    bigquery = None
    has_connector = False


class BigQuery:

    def __init__(self, config: BigQueryConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.project_name = config.project_name
        self.dataset_name = config.dataset_name
        self.retry_timeout = config.retry_timeout

        self.events_table_name = config.events_table

        self.metrics_table_name = config.metrics_table
        self.shared_metrics_table_name = f'{self.metrics_table_name}_shared'
        self.custom_metrics_table_name = f'{self.metrics_table_name}_custom'

        self.stream_metrics_table_name = config.streams_table

        self.errors_table_name = f'{self.metrics_table_name}_errors'
        self.experiments_table_name = config.experiments_table
        self.variants_table_name = f'{self.experiments_table_name}_variants'
        self.mutations_table_name = f'{self.experiments_table_name}_mutations'

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.credentials = None
        self.client = None

        self._events_table = None
        self._streams_table = None
        self._errors_table = None
        self._metrics_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._custom_metrics_table = None
        self._shared_metrics_table = None

        self._loop = asyncio.get_event_loop()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to Google Cloud - Loading account config from - {self.service_account_json_path}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')

        self.client = bigquery.Client.from_service_account_json(self.service_account_json_path)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Loaded account config from - {self.service_account_json_path}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.stream_metrics_table_name} - if not exists')

        rows = []
        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streans - {stage_name}:{stream.stream_set_id}')

            if self._streams_table is None:

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.stream_metrics_table_name} - if not exists')

                table_schema = [
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('group', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('median', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('mean', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variance', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('stdev','FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('minimum', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('maximum', 'FLOAT64', mode='REQUIRED')
                ]

                for quantile in stream.quantiles:
                    table_schema.append(
                        bigquery.SchemaField(f'{quantile}', 'FLOAT64')
                    )

                table_reference = bigquery.TableReference(
                    bigquery.DatasetReference(
                        self.project_name,
                        self.dataset_name
                    ),
                    self.stream_metrics_table_name
                )

                streams_table = bigquery.Table(
                    table_reference,
                    schema=table_schema
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.create_table,
                        streams_table,
                        exists_ok=True
                    )
                )

                self._streams_table = streams_table

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.stream_metrics_table_name} - if not exists')
            
            for group_name, group in stream.grouped:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams Group - {group_name}:{stream.stream_set_id}')
                rows.append({
                    'name': f'{stage_name}_stream',
                    'stage': stage_name,
                    'group': group_name,
                    **group
                })

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                self._streams_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.metrics_table_name} - if not exists')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Experiments to table - {self.experiments_table_name}')

        if self._experiments_table is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.experiments_table_name} - if not exists')
            
            experiments_table_name = f'{self.project_name}.{self.dataset_name}.{self.experiments_table_name}'

            table_schema = bigquery.Table(
                experiments_table_name,
                schema=[
                    bigquery.SchemaField('experiment_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('experiment_randomized', 'BOOLEAN', mode='REQUIRED'),
                    bigquery.SchemaField('experiment_completed', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('experiment_succeeded', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('experiment_failed', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('experiment_median_aps', 'FLOAT64', mode='REQUIRED'),
                ])

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.experiments_table_name
            )

            self._experiments_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    self._experiments_table,
                    exists_ok=True
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.experiments_table_name} - if not exists')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.experiments_table_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table_reference,
                experiment_metrics.experiments,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.experiments_table_name}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Variants to table - {self.variants_table_name}')

        if self._variants_table is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.variants_table_name} - if not exists')
            
            variants_table_name = f'{self.project_name}.{self.dataset_name}.{self.variants_table_name}'

            table_schema = bigquery.Table(
                variants_table_name,
                schema=[
                    bigquery.SchemaField('variant_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('variant_experiment', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('variant_weight', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_distribution', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('variant_distribution_interval', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_ratio_completed', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_ratio_succeeded', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_ratio_failed', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_ratio_aps', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_completed', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_succeeded', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_failed', 'INT64', mode='REQUIRED'),
                    bigquery.SchemaField('variant_actions_per_second', 'FLOAT64', mode='REQUIRED'),
                ])

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.variants_table_name
            )

            self._variants_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    self._variants_table,
                    exists_ok=True
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.variants_table_name} - if not exists')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.variants_table_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table_reference,
                experiment_metrics.variants,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.variants_table_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Mutations to table - {self.mutations_table_name}')

        if self._mutations_table is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.mutations_table_name} - if not exists')
            
            mutations_table_name = f'{self.project_name}.{self.dataset_name}.{self.mutations_table_name}'

            table_schema = bigquery.Table(
                mutations_table_name,
                schema=[
                    bigquery.SchemaField('mutation_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('mutation_experiment_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('mutation_variant_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('mutation_chance', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('mutation_targets', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('mutation_type', 'STRING', mode='REQUIRED'),
                ])

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.mutations_table_name
            )

            self._mutations_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    self._mutations_table,
                    exists_ok=True
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.mutations_table_name} - if not exists')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.mutations_table_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table_reference,
                experiment_metrics.mutations,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.mutations_table_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):
        
        if self._events_table is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.events_table_name} - if not exists')
            
            events_table_name = f'{self.project_name}.{self.dataset_name}.{self.events_table_name}'

            table_schema = bigquery.Table(
                events_table_name,
                schema=[
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('time', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('succeeded', 'BOOLEAN', mode='REQUIRED'),
                    bigquery.SchemaField('error', 'STRING')
                ])

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.events_table_name
            )

            self._events_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    self._events_table,
                    exists_ok=True
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.events_table_name} - if not exists')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.events_table_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table_reference,
                [event.record for event in events],
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        if self.shared_metrics_table_name is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.shared_metrics_table_name} - if not exists')

            table_schema = [
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('group', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('total', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('succeeded', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('failed', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('actions_per_second', 'FLOAT64', mode='REQUIRED')
            ]

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.shared_metrics_table_name
            )

            shared_metrics_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    shared_metrics_table,
                    exists_ok=True
                )
            )

            self._shared_metrics_table = shared_metrics_table

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.shared_metrics_table_name} - if not exists')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.shared_metrics_table_name} - if not exists')

        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            rows.append({
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            })

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                self._shared_metrics_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.shared_metrics_table_name} - if not exists')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.metrics_table_name} - if not exists')

        rows = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            if self._metrics_table is None:

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.metrics_table_name} - if not exists')

                table_schema = [
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('group', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('median', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('mean', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('variance', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('stdev','FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('minimum', 'FLOAT64', mode='REQUIRED'),
                    bigquery.SchemaField('maximum', 'FLOAT64', mode='REQUIRED')
                ]

                for quantile in metrics_set.quantiles:
                    table_schema.append(
                        bigquery.SchemaField(f'{quantile}', 'FLOAT64')
                    )

                for custom_field_name, bigquery_schema_field in metrics_set.custom_schemas.items():
                    table_schema.append(
                        custom_field_name, 
                        bigquery_schema_field
                    ) 

                table_reference = bigquery.TableReference(
                    bigquery.DatasetReference(
                        self.project_name,
                        self.dataset_name
                    ),
                    self.metrics_table_name
                )

                metrics_table = bigquery.Table(
                    table_reference,
                    schema=table_schema
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.create_table,
                        metrics_table,
                        exists_ok=True
                    )
                )

                self._metrics_table = metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.metrics_table_name} - if not exists')
            
            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {group_name}:{group.metrics_group_id}')
                rows.append({
                    'group': group_name,
                    **group.record
                })

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                self._metrics_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.metrics_table_name} - if not exists')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics - Project: {self.project_name} - Dataset: {self.dataset_name}')

        if self._custom_metrics_table is None:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.custom_metrics_table_name} - if not exists')

            table_schema = [
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('group', 'STRING', mode='REQUIRED')
            ]

            for metrics_set in metrics_sets:
                for custom_metric in metrics_set.custom_metrics.values():

                    if custom_metric.metric_type == MetricType.COUNT:
                        table_schema.append(
                            bigquery.SchemaField(
                                custom_metric.metric_name, 
                                'INTEGER', 
                                mode='REQUIRED'
                            )
                        )

                    else:
                        table_schema.append(
                            bigquery.SchemaField(
                                custom_metric.metric_name, 
                                'FLOAT64', 
                                mode='REQUIRED'
                            )
                        )

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self._custom_metrics_table_name
            )

            custom_metrics_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    custom_metrics_table,
                    exists_ok=True
                )
            )

            self._custom_metrics_table = custom_metrics_table

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.custom_metrics_table_name} - if not exists')

        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')   
        
            rows.append({
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'custom',
                **{
                    custom_metric.metric_name: custom_metric.metric_value for custom_metric in metrics_set.custom_metrics.values()
                }
            })

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                self._custom_metrics_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics - Project: {self.project_name} - Dataset: {self.dataset_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Errors Metrics Set - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.errors_table_name} - if not exists')

        rows = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            if self._errors_table is None:

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.errors_table_name} - if not exists')

                table_schema = [
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('error_message', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('error_count', 'INTEGER', mode='REQUIRED'),
                ]

                table_reference = bigquery.TableReference(
                    bigquery.DatasetReference(
                        self.project_name,
                        self.dataset_name
                    ),
                    self.errors_table_name
                )

                errors_table = bigquery.Table(
                    table_reference,
                    schema=table_schema
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.create_table,
                        errors_table,
                        exists_ok=True
                    )
                )

                self._errors_table = errors_table

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.errors_table_name} - if not exists')

            rows.extend([
                {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                } for error in metrics_set.errors
            ])

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                self._errors_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics - Project: {self.project_name} - Dataset: {self.dataset_name} - Table: {self.errors_table_name} - if not exists')

    async def close(self):

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')

        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )