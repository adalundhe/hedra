import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup

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
        self.errors_table_name = f'{self.metrics_table_name}_errors'
        self.custom_fields = config.custom_fields or []


        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.credentials = None
        self.client = None
        self._events_table = None
        self._errors_table = None
        self._metrics_table = None
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

    async def submit_common(self, metrics_groups: List[MetricsGroup]):
        if self._groups_metrics_table is None:
            table_schema = [
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('total', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('succeeded', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('failed', 'INTEGER', mode='REQUIRED')
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
        for metrics_group in metrics_groups:
            rows.append({
                'name': metrics_group.name,
                'stage': metrics_group.stage,
                **metrics_group.common_stats
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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        rows = []
        for metrics_group in metrics:

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

                for quantile in metrics_group.quantiles:
                    table_schema.append(
                        bigquery.SchemaField(f'{quantile}', 'FLOAT')
                    )

                for custom_field_name, bigquery_schema_field in metrics_group.custom_schemas.items():
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
            
            
            for timing_group_name, timing_group in metrics_group.groups.items():
                rows.append({
                    'group': timing_group_name,
                    **timing_group.record
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

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        rows = []
        for metrics_group in metrics_groups:

            if self._errors_table is None:
                table_schema = [
                    bigquery.SchemaField('metric_name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('metric_stage', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('error_message', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('error_count', 'INTEGER', mode='REQUIRED'),
                ]

                table_reference = bigquery.TableReference(
                    bigquery.DatasetReference(
                        self.project_name,
                        self.dataset_name
                    ),
                    f'{self.metrics_table_name}_errors'
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
                    'metric_name': metrics_group.name,
                    'metric_stage': metrics_group.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                } for error in metrics_group.errors
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