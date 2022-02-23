import datadog
import uuid

from async_tools.functions.awaitable import awaitable


class Metric:

    def __init__(self, metric):
        self.name = metric.get('name')
        self.values = metric.get('values')
        self.host = metric.get('host')
        self.tags = metric.get('tags', [])
        self.type = metric.get('metric_type', 'gauge')
        self.options = metric.get('options', {})

    async def get(self) -> dict:
        if self.options.get('window'):
            response = await awaitable(
                datadog.api.Metric.list, 
                self.options.get('from_time')
            )

        elif self.options.get('search'):
            response = await awaitable(
                datadog.api.Metric.get_all, 
                **self.options.get('filters', {})
            )

        else:
            response = await awaitable(
                datadog.api.Metric.query,
                query=self.name,
                start=self.options.get('from_time'),
                end=self.options.get('to_time')
            )

        return response

    async def create(self) -> dict:
        tags = [
            '{tag_name}:{tag_value}'.format(
                tag_name=tag.get('name'),
                tag_value=tag.get('value')
            ) for tag in self.tags
        ]
        
        response = await awaitable(
            datadog.api.Metric.send,
            metrics=[{
                'metric': self.name,
                'points': self.values,
                'host': self.host,
                'tags': tags,
                'type': self.type
            }]
        )

        return response

    async def update(self) -> dict:
        return {}

    async def delete(self) -> dict:
        return {}

    