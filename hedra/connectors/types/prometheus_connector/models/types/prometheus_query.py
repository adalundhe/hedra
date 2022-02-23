import datetime
from prometheus_api_client import (
    PrometheusConnect
)


class PrometheusQuery:

    def __init__(self, query):

        self.name = query.name
        self.type = query.type
        self.labels = query.labels
        self.operators = query.operators
        self.options = query.options

    async def assemble_value_query(self) -> dict:
        return {
                'metric_name': self.name,
                'label_config': self.labels
            }

    async def assemble_range_query(self) -> dict:
        if self.options.get('time_chunk'):
            time_chunk = datetime.timedelta(**self.options.get('time_chunk'))
        else:
            time_chunk = None

        date_range = await self._create_date_range_from_options()

        return {
            'metric_name': self.name,
            'label_config': self.labels,
            'chunk_size': time_chunk,
            **date_range
        }

    async def assemble_aggregation_query(self) -> dict:
        return {
            'query': self.name,
            'operations': self.operators,
            'step': self.options.get('step'),
            **self._create_date_range_from_options()
        }

    async def assemble_custom_range_query(self) -> dict:

        date_range = await self._create_date_range_from_options()

        return {
            'query': await self._create_custom_query_string(),
            'step': self.options.get('step'),
            **date_range
        }

    async def assemble_custom_query(self) -> dict:
        return {
            'query': await self._create_custom_query_string()
        }

    async def _create_date_range_from_options(self) -> dict:
        date_range_dict = {}

        if self.options.get('start_time'):
            date_range_dict['start_time'] = datetime.datetime.strptime(
                self.options.get('start_time'),
                '%Y-%m-%dT%H:%M:%S'
            )

        if self.options.get('end_time'):
            date_range_dict['end_time'] = datetime.datetime.strptime(
                self.options.get('end_time'),
                '%Y-%m-%dT%H:%M:%S'
            )

        return date_range_dict

    async def _create_custom_query_string(self) -> str:

        query_string = self.name

        if self.labels:
            query_labels = ', '.join([
                f'{label_name}="{label_value}"' for label_name, label_value in self.labels.items()
            ])

            query_string = ''.join([
                query_string, 
                '{',
                query_labels,
                '}'
            ])

        if self.options.get('time_range'):
            query_string = f'{query_string}[{self.options["time_range"]}]'
            
        return query_string

        

            
                