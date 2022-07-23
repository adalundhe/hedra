import asyncio
from typing import Any, List

try:
    import snowflake.connector
    has_connector = True

except ImportError:
    has_connector = False

class Snowflake:

    def __init__(self, config: Any) -> None:
        self.username = config.username
        self.account_id = config.account_id
        self.password = config.password
        self.private_key = config.private_key
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.schema
        self.events_table = config.events_table
        self.metrics_table = config.metrics_table
        self.custom_fields = config.custom_fields

        self.connection = None
        self.cursor = None
        self._loop = asyncio.get_event_loop()

        # For events we only allow storing a few types as to avoid 
        # potentially inserting dangerous data.
        self.events_fields= {
            'time': 'DECIMAL',
            'succeeded': 'BOOLEAN',
            'failed': 'BOOLEAN'
        }
        self.metrics_fields = {
            'total': 'BIGINT',
            'succeeded': 'BIGINT',
            'failed': 'BIGINT',
            'median': 'DECIMAL',
            'mean': 'DECIMAL',
            'variance': 'DECIMAL',
            'stdev': 'DECIMAL',
            'minimum': 'DECIMAL',
            'maximum': 'DECIMAL',
            'quantiles': 'DECIMAL',
            **self.custom_fields
        }

    async def connect(self):
        self.connection = await self._loop.run_in_executor(
            None,
            snowflake.connector.connect,
            user=self.username,
            password=self.password,
            account=self.account_id,
            private_key=self.private_key,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )

        self.cursor = await self._loop.run_in_executor(
            self.connection.cursor
        )

        fields = ['id INTEGER default id_seq.nextval']

        for field, field_type in self.events_fields.items():
            fields.append(f'{field} {field_type}')

        fields = ', '.join(fields)

        await self._loop.run_in_executor(
            None,
            self.cursor.execute,
            f'CREATE TABLE IF NOT EXISTS {self.events_table} ({fields});'
        )

        fields = ['id INTEGER default id_seq.nextval']

        for field, field_type in self.metrics_fields.items():
            fields.append(f'{field} {field_type}')

        fields = ', '.join(fields)

        await self._loop.run_in_executor(
            None,
            self.cursor.execute,
            f'CREATE TABLE IF NOT EXISTS {self.metrics_table} ({fields});'
        )

    async def submit_events(self, events: List[Any]):

        for event in events:
    
            event_fields = []
            event_values = []
            
            for field, value in event.record.items():
                event_fields.append(field)
                event_values.append(value)

            event_fields = ', '.join(event_fields)
            event_values = ', '.join(event_values)

            insert_string = f'INSERT INTO {self.events_table}({event_fields}) VALUES ({event_values});'
            await self._loop.run_in_executor(
                None,
                self.cursor.execute,
                insert_string
            )

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
    
            metric_fields = []
            metric_values = []
            
            for field, value in metric.record.items():
                metric_fields.append(field)
                metric_values.append(value)

            metric_fields = ', '.join(metric_fields)
            metric_values = ', '.join(metric_values)

            insert_string = f'INSERT INTO {self.metrics_table}({metric_fields}) VALUES ({metric_values});'
            await self._loop.run_in_executor(
                None,
                self.cursor.execute,
                insert_string
            )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.connection.close
        )