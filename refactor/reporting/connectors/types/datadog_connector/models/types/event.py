import datadog
from async_tools.functions.awaitable import awaitable


class Event:

    def __init__(self, event):
        self.id = event.get('id')
        self.type = event.get('type')
        self.name = event.get('name')
        self.text = event.get('text')
        self.alert_type = event.get('alert_type', 'info')
        self.aggregation_key = event.get('aggregation_key')
        self.source = event.get('source')
        self.timestamp = event.get('timestamp')
        self.priority = event.get('priority', 'normal')
        self.tags = event.get('tags', [])
        self.host = event.get('host')
        self.options = event.get('options', {})

    async def get(self) -> dict:  
        if self.options.get('search'):
            response = await awaitable(
                datadog.api.Event.query, 
                **self.options.get('filters', {})
            )
        else:
            response = await awaitable(
                datadog.api.Event.get, 
                self.id
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
            datadog.api.Event.create,
            title=self.name,
            text=self.text,
            alert_type=self.alert_type,
            aggregation_key=self.aggregation_key,
            device_name=self.source,
            date_happened=self.timestamp,
            priority=self.priority,
            tags=tags,
            host=self.host
        )
        

        return response

    async def update(self) -> dict:
        return {}

    async def delete(self) -> dict:
        return {}
