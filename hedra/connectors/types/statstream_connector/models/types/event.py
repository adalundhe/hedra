from .tag_collection import TagCollection


class Event:

    def __init__(self, event):
        self.stream_name = event.get('stream_name')
        self.metadata = {
            'event_name': event.get('event_name'),
            'event_host': event.get('event_host'),
            'event_url': event.get('event_url'),
            'event_type': event.get('event_type'),
            'event_context': event.get('event_context')
        }
        self.key = event.get('event_name')
        self.value = event.get('event_metric')
        self.bin = event.get('event_status')
        self.tags = TagCollection(tags=event.get('event_tags'))

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.value

    async def to_dict(self):
        return {
            'stream_name': self.stream_name,
            'metadata': {
                **self.metadata,
                'event_tags': await self.tags.to_dict_list()
            },
            'key': self.key,
            'value': self.value,
            'bin': self.bin
        }