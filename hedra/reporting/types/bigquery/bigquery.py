import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:
    from google.cloud import bigquery
    from .bigquery_config import BigQueryConfig
    has_connector = True

except ImportError:
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
        self.custom_fields = config.custom_fields or []


        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.credentials = None
        self.client = None
        self._events_table = None
        self._metrics_table = None

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
                    bigquery.SchemaField('succeeded', 'BOOLEAN', mode='REQUIRED')
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

    async def submit_metrics(self, metrics: List[Metric]):
        metric = metrics[0]
        if self._metrics_table is None:

            table_schema = [
                bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('stage', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('total', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('succeeded', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('failed', 'INTEGER', mode='REQUIRED'),
                bigquery.SchemaField('median', 'FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('mean', 'FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('variance', 'FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('stdev','FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('minimum', 'FLOAT', mode='REQUIRED'),
                bigquery.SchemaField('maximum', 'FLOAT', mode='REQUIRED')
            ]

            for quantile in metric.quantiles:
                table_schema.append(
                    bigquery.SchemaField(f'{quantile}', 'FLOAT')
                )

            for custom_field_name, bigquery_schema_field in self.custom_fields:
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

            self._metrics_table = bigquery.Table(
                table_reference,
                schema=table_schema
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    self._metrics_table,
                    exists_ok=True
                )
            )

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.insert_rows_json,
                table_reference,
                [metric.record for metric in metrics],
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