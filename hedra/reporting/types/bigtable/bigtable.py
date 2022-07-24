import asyncio
import uuid
from datetime import datetime
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:
    from google.cloud import bigtable
    from google.auth.credentials import Credentials
    from .bigtable_config import BigTableConfig
    has_connector = True

except ImportError:
    bigtable = None
    Credentials = None
    BigTableConfig = None
    has_connector = False


class BigTable:

    def __init__(self, config: BigTableConfig) -> None:
        self.token = config.token
        self.instance_name = config.instance
        self.instance = None
        self.events_table_id = config.events_table
        self.metrics_table_id = config.metrics_table

        self._events_table = None
        self._metrics_table = None
        self._events_table_columns = None
        self._metrics_table_columns = None
        self._events_column_family_id = f'{self.events_table_id}_columns'
        self._metrics_column_family_id = f'{self.metrics_table_id}_columns'
        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.credentials = Credentials()
        self.credentials.token = self.token

        self.client = bigtable.Client(credentials=self.credentials)
        self.instance = self.client.instance(self.instance_name)
        
        await self._loop.run_in_executor(
            None,
            self.instance.create
        )

    async def submit_events(self, events: List[BaseEvent]):
        self._events_table = self.instance.table(self.events_table_id)
        
        try:
            await self._loop.run_in_executor(
                None,
                self._events_table.create
            )

        except Exception:
            pass

        self._events_table_columns = self._events_table.column_family(
            self._events_column_family_id
        )

        try:
            await self._loop.run_in_executor(
                None,
                self._events_table_columns.create
            )
        except Exception:
            pass

        rows = []
        for event in events:
            row_key = f'{event.name}_{str(uuid.uuid4())}'
            row = self._events_table.direct_row(row_key)

            for field, value in event.record.items():
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._events_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            None,
            self._events_table.mutate_rows,
            rows
        )
    
    async def submit_metrics(self, metrics: List[Metric]):
        self._metrics_table = self.instance.table(self.metrics_table_id)
        
        try:
            await self._loop.run_in_executor(
                None,
                self._metrics_table.create
            )

        except Exception:
            pass

        self._metrics_table_columns = self._metrics_table.column_family(
            self._metrics_column_family_id
        )

        try:
            await self._loop.run_in_executor(
                None,
                self._metrics_table_columns.create
            )
        except Exception:
            pass

        rows = []
        for metric in metrics:
            row_key = f'{metric.name}_{str(uuid.uuid4())}'
            row = self._metrics_table.direct_row(row_key)

            for field, value in metric.record.items():
                if not isinstance(value, bytes):
                    value = f'{value}'.encode()

                row.set_cell(
                    self._metrics_column_family_id,
                    field,
                    value,
                    timestamp=datetime.now()
                )

            rows.append(row)

        await self._loop.run_in_executor(
            None,
            self._metrics_table.mutate_rows,
            rows
        )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.client.close
        )
        