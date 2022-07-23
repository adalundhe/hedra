from typing import Any, List


try:
    import aiosqlite
    has_connector = True

except ImportError:
    has_connector = False



class SQLite:

    def __init__(self, config) -> None:
        self.path = config.path
        self.events_table = config.events_table
        self.metrics_table = config.metrics_table
        self.database = None
        self._events_table = None
        self._metrics_table = None
        self.custom_fields = config.custom_fields

        self.events_fields= {
            'name': 'TEXT',
            'stage': 'TEXT',
            'time': 'REAL',
            'succeeded': 'INTEGER'
        }
        self.metrics_fields = {
            'name': 'TEXT',
            'stage': 'TEXT',
            'total': 'INTEGER',
            'succeeded': 'INTEGER',
            'failed': 'INTEGER',
            'median': 'REAL',
            'mean': 'REAL',
            'variance': 'REAL',
            'stdev': 'REAL',
            'minimum': 'REAL',
            'maximum': 'REAL',
            'quantiles': 'REAL',
            **self.custom_fields
        }

    async def connect(self):
        self.database = await aiosqlite.connect(self.path) 

        fields = []

        for field, field_type in self.events_fields.items():
            fields.append(f'{field} {field_type}')

        fields = ', '.join(fields)

        await self.database.execute(f'CREATE TABLE IF NOT EXISTS {self.events_table} ({fields});')
        
        fields = []

        for field, field_type in self.metrics_fields.items():
            fields.append(f'{field} {field_type}')

        fields = ', '.join(fields)

        await self.database.execute(f'CREATE TABLE IF NOT EXISTS {self.metrics_table} ({fields});')

    
    async def submit_events(self, events: List[Any]):

        for event in events:

            event_fields = []
            event_values = []
            
            for field, value in event.record.items():

                if isinstance(value, bool):
                    value = int(value)

                elif isinstance(value, str):
                    value = f'"{value}"'

                event_fields.append(field)
                event_values.append(value)

            event_fields = ', '.join(event_fields)
            event_values = ', '.join(event_values)

            await self.database.execute(f'INSERT INTO {event.fields} {self.events_table} VALUES ({event.values})')

        await self.database.commit()


    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:

            metric_fields = []
            metric_values = []
            
            for field, value in metric.record.items():

                if isinstance(value, bool):
                    value = int(value)

                elif isinstance(value, str):
                    value = f'"{value}"'

                metric_fields.append(field)
                metric_values.append(value)

            metric_fields = ', '.join(metric_fields)
            metric_values = ', '.join(metric_values)

            await self.database.execute(f'INSERT INTO {metric.fields} {self.metrics_table} VALUES ({metric.values})')

        await self.database.commit()

    async def close(self):
        await self.database.close()