import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import functools
from typing import List
from numpy import float32, float64, int16, int32, int64

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

try:
    from google.cloud import bigquery
    from .bigquery_config import BigQueryConfig
    has_connector = True

except Exception:
    bigquery = None
    BigQueryConfig = None
    has_connector = False


class BigQuery:

    def __init__(self, config: BigQueryConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.project_name = config.project_name
        self.dataset_name = config.dataset_name
        self.retry_timeout = config.retry_timeout
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.group_metrics_table_name = f'{self.metrics_table_name}_group_metrics'
        self.custom_metrics_table_names = {}
        self.errors_table_name = f'{self.metrics_table_name}_errors'
        self.custom_fields = config.custom_fields or []


        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.credentials = None
        self.client = None
        self._events_table = None
        self._errors_table = None
        self._metrics_table = None
        self._custom_metrics_tables = {}
        self._groups_metrics_table = None

        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = bigquery.Client.from_service_account_json(self.service_account_json_path)

    async def submit_events(self, events: List[BaseEvent]):
        
        if self._events_table is None:
            
            events_table_name = f'{self.project_name}.{self.dataset_name}.{self.events_table_name}'

            table_schema = bigquery.Table(
                events_table_name,
                schema=[
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('time', 'FLOAT', mode='REQUIRED'),
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

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        if self._groups_metrics_table is None:
            table_schema = [
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('group', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('total', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('succeeded', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('failed', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('actions_per_second', 'FLOAT', mode='REQUIRED')
            ]

            table_reference = bigquery.TableReference(
                bigquery.DatasetReference(
                    self.project_name,
                    self.dataset_name
                ),
                self.group_metrics_table_name
            )

            group_metrics_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    group_metrics_table,
                    exists_ok=True
                )
            )

            self._groups_metrics_table = group_metrics_table

        rows = []
        for metrics_set in metrics_sets:
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
                self._groups_metrics_table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        rows = []
        for metrics_set in metrics:

            if self._metrics_table is None:

                table_schema = [
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('group', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('median', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('mean', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('variance', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('stdev','FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('minimum', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('maximum', 'FLOAT', mode='REQUIRED')
                ]

                for quantile in metrics_set.quantiles:
                    table_schema.append(
                        bigquery.SchemaField(f'{quantile}', 'FLOAT')
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
            
            
            for group_name, group in metrics_set.groups.items():
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

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        rows = defaultdict(list)
        for metrics_set in metrics_sets:
            
            for custom_metrics_group_name, custom_metrics_group in metrics_set.custom_metrics.items():

                custom_metrics_table_name = f'{self.metrics_table_name}_{custom_metrics_group_name}_metrics'

                if self._custom_metrics_tables.get(custom_metrics_table_name) is None:
                    table_schema = [
                        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                        bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                        bigquery.SchemaField('group', 'STRING', mode='REQUIRED')
                    ]

                    for field, value in custom_metrics_group.items():

                        if isinstance(value, (int, int16, int32, int64)):
                            table_schema.append(
                                bigquery.SchemaField(field, 'INTEGER', mode='REQUIRED')
                            )

                        elif isinstance(value, (float, float32, float64)):
                            table_schema.append(
                                bigquery.SchemaField(field, 'FLOAT', mode='REQUIRED')
                            )

                    table_reference = bigquery.TableReference(
                        bigquery.DatasetReference(
                            self.project_name,
                            self.dataset_name
                        ),
                        custom_metrics_table_name
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

                    self._custom_metrics_tables[custom_metrics_table_name] = custom_metrics_table
            
                rows[custom_metrics_table_name].append({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_metrics_group_name,
                    **custom_metrics_group
                })
        
        for table_name, rows in rows.items():

            table = self._custom_metrics_tables.get(table_name)

            await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table,
                rows,
                retry=bigquery.DEFAULT_RETRY.with_predicate(
                    lambda exc: exc is not None
                ).with_deadline(
                    self.retry_timeout
                )
            )
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        rows = []
        for metrics_set in metrics_sets:

            if self._errors_table is None:
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

    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )