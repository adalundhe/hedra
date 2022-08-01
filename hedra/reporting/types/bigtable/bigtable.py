import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import uuid
from datetime import datetime
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

try:
    from google.cloud import bigtable
    from google.auth import load_credentials_from_file
    from .bigtable_config import BigTableConfig
    has_connector = True

except Exception:
    bigtable = None
    Credentials = None
    BigTableConfig = None
    has_connector = False


class BigTable:

    def __init__(self, config: BigTableConfig) -> None:
        self.service_account_json_path = config.service_account_json_path
        self.instance_id = config.instance_id
        self.events_table_id = config.events_table
        self.metrics_table_id = config.metrics_table
        self.groups_metrics_table_id = f'{self.metrics_table_id}_group_metrics'
        self.custom_metrics_table_ids = {}
        self.errors_table_id = f'{self.metrics_table_id}_errors'

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._events_column_family_id = f'{self.events_table_id}_columns'
        self._metrics_column_family_id = f'{self.metrics_table_id}_columns'
        self._group_metrics_column_family_id = f'{self.metrics_table_id}_group_metrics_columns'
        self._custom_metrics_column_family_ids = {}
        self._errors_column_family_id = f'{self.metrics_table_id}_errors_columns'

        self.instance = None

        self._events_table = None
        self._group_metrics_table = None
        self._custom_metrics_tables = {}
        self._metrics_table = None
        self._errors_table = None

        self._events_table_columns = None
        self._group_metrics_table_columns = None
        self._custom_metrics_table_columns = {}
        self._metrics_table_columns = None
        self._errors_table_columns = None

        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        credentials, project_id = load_credentials_from_file(self.service_account_json_path)
        self.client = bigtable.Client(
            project=project_id,
            credentials=credentials,
            admin=True
        )
        self.instance = self.client.instance(self.instance_id)

    async def submit_events(self, events: List[BaseEvent]):
        self._events_table = self.instance.table(self.events_table_id)
        
        try:
            await self._loop.run_in_executor(
                self._executor,
                self._events_table.create
            )

        except Exception:
            pass

        self._events_table_columns = self._events_table.column_family(
            self._events_column_family_id
        )

        try:
            await self._loop.run_in_executor(
                self._executor,
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
            self._executor,
            self._events_table.mutate_rows,
            rows
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        group_metrics_table = self.instance.table(self.groups_metrics_table_id)
            
        try:
            await self._loop.run_in_executor(
                self._executor,
                group_metrics_table.create
            )

        except Exception:
            pass

        self._metrics_table_columns = group_metrics_table.column_family(
            self._group_metrics_column_family_id
        )

        
        try:
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )
        except Exception:
            pass


        rows = []
        for metrics_set in metrics_sets:
            
            row_key = f'{self.groups_metrics_table_id}_{str(uuid.uuid4())}'
            row = group_metrics_table.direct_row(row_key)

            group_metrics_record = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            }

            for field, value in group_metrics_record.items():
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
            self._executor,
            group_metrics_table.mutate_rows,
            rows
        )
    
    async def submit_metrics(self, metrics: List[MetricsSet]):
        metrics_table = self.instance.table(self.metrics_table_id)
            
        try:
            await self._loop.run_in_executor(
                self._executor,
                metrics_table.create
            )

        except Exception:
            pass

        self._metrics_table_columns = metrics_table.column_family(
            self._metrics_column_family_id
        )
        
        try:
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )
        except Exception:
            pass
        
        rows = []
        for metrics_set in metrics:

            for group_name, group in metrics_set.groups.items():

                row_key = f'{self.metrics_table_id}_{str(uuid.uuid4())}'
                row = metrics_table.direct_row(row_key)

                record = {
                    'group': group_name,
                    **group.record
                }

                for field, value in record.items():
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
            self._executor,
            metrics_table.mutate_rows,
            rows
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        rows = defaultdict(list)
        for metrics_set in metrics_sets:

            for custom_group_name, custom_group in metrics_set.custom_metrics.items():

                custom_metrics_table_id = f'{self.metrics_table_id}_{custom_group_name}_metrics'
                custom_metrics_table = self.instance.table(custom_metrics_table_id)

                try:
                    await self._loop.run_in_executor(
                        self._executor,
                        custom_metrics_table.create
                    )
                
                except Exception:
                    pass

                self._custom_metrics_tables[custom_metrics_table_id] = custom_metrics_table

                custom_metrics_table_columns = custom_metrics_table.column_family(
                    custom_metrics_table_id
                )

                try:
                    await self._loop.run_in_executor(
                        self._executor,
                        custom_metrics_table_columns.create
                    )
                except Exception:
                    pass

                row_key = f'{custom_metrics_table_id}_{str(uuid.uuid4())}'
                custom_metrics_table_row = custom_metrics_table.direct_row(row_key)

                custom_metrics_row_data = {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **custom_group
                }

                for field, value in custom_metrics_row_data.items():
                    custom_metrics_table_row.set_cell(
                        field,
                        f'{value}'.encode()
                    )
                
                rows[custom_metrics_table_id].append(custom_metrics_table_row)

        for table_id, table_rows in rows.items():
            table = self._custom_metrics_tables.get(table_id)
            await self._loop.run_in_executor(
                self._executor,
                table.mutate_rows,
                table_rows
            )


    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        errors_table = self.instance.table(self.errors_table_id)
            
        try:
            await self._loop.run_in_executor(
                self._executor,
                errors_table.create
            )

        except Exception:
            pass

        self._errors_table = errors_table

        self._errors_table_columns = errors_table.column_family(
            self._errors_column_family_id
        )

        try:
            await self._loop.run_in_executor(
                self._executor,
                self._metrics_table_columns.create
            )
        except Exception:
            pass
        
        rows = []
        for metrics_set in metrics_sets:
            
            for error in metrics_set.errors:

                row_key = f'{self.errors_table_id}_{str(uuid.uuid4())}'
                errors_row = errors_table.direct_row(row_key)

                error_message = error.get('message')
                error_count = error.get('count')

                errors_row.set_cell(
                    self._metrics_column_family_id,
                    'error_type',
                    f'{error_message}'.encode()
                )

                errors_row.set_cell(
                    self._metrics_column_family_id,
                    'error_count',
                    f'{error_count}'.encode()
                )

                rows.append(errors_row)
                
        await self._loop.run_in_executor(
            self._executor,
            errors_table.mutate_rows,
            rows
        )

    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )
        