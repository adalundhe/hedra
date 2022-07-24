import asyncio
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:
    from google.cloud import bigquery
    from google.auth.credentials import Credentials
    from .bigquery_config import BigQueryConfig
    has_connector = True

except ImportError:
    bigquery = None
    BigQueryConfig = None
    Credentials = None
    has_connector = False


class BigQuery:

    def __init__(self, config: BigQueryConfig) -> None:
        self.token = config.token
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.custom_fields = config.custom_fields or []
        self.credentials = None
        self.client = None

        self._events_table = None

        self._metrics_table = bigquery.Table(
            self.metrics_table_name,
            schema=[]
        )

        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.credentials = Credentials()
        self.credentials.token = self.token

        self.client = bigquery.Client(credentials=self.credentials)


        try:
            await self._loop.run_in_executor(
                None,
                self.client.get_table,
                self._metrics_table
            )

        except Exception:
            await self._loop.run_in_executor(
                None,
                self.client.create_table,
                self._metrics_table
            )

    async def submit_events(self, events: List[BaseEvent]):
        
        if self._events_table is None:

            self._events_table = bigquery.Table(
                self.events_table_name,
                schema=[
                    bigquery.SchemaField('name', 'STRING', required=True),
                    bigquery.SchemaField('stage', 'STRING', required=True),
                    bigquery.SchemaField('time', 'FLOAT', required=True),
                    bigquery.SchemaField('succeeded', 'BOOLEAN', required=True)
                ])

            try:
                await self._loop.run_in_executor(
                    None,
                    self.client.get_table,
                    self._events_table
                )

            except Exception:
                await self._loop.run_in_executor(
                    None,
                    self.client.create_table,
                    self._events_table
                )

        await self._loop.run_in_executor(
            None,
            self.client.insert_rows_json,
            [event.record for event in events]
        )

    async def submit_metrics(self, metrics: List[Metric]):
        metric = metrics[0]
        if self._metrics_table is None:
            table_schema = [
                bigquery.SchemaField('name', 'STRING', required=True),
                bigquery.SchemaField('stage', 'STRING', required=True),
                bigquery.SchemaField('total', 'INTEGER', required=True),
                bigquery.SchemaField('succeeded', 'INTEGER', required=True),
                bigquery.SchemaField('failed', 'INTEGER', required=True),
                bigquery.SchemaField('median', 'FLOAT', required=True),
                bigquery.SchemaField('mean', 'FLOAT', required=True),
                bigquery.SchemaField('variance', 'FLOAT', required=True),
                bigquery.SchemaField('stdev','FLOAT', required=True),
                bigquery.SchemaField('minimum', 'FLOAT', required=True),
                bigquery.SchemaField('maximum', 'FLOAT', required=True)
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

            self._metrics_table = bigquery.Table(
                self.metrics_table_name,
                schema=table_schema
            )

            try:
                await self._loop.run_in_executor(
                    None,
                    self.client.get_table,
                    self._metrics_table
                )

            except Exception:
                await self._loop.run_in_executor(
                    None,
                    self.client.create_table,
                    self._metrics_table
                )

        await self._loop.run_in_executor(
            None,
            self.client.insert_rows_json,
            [metric.record for metric in metrics]
        )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.client.close
        )