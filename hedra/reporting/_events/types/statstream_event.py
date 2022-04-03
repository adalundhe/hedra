from .base_event import BaseEvent
from .event_tags import EventTagCollection


class StatStreamEvent(BaseEvent):
    fields = {
        'event_name': str,
        'event_host': str,
        'event_url': str,
        'event_type': str,
        'event_tags': list,
        'event_context': str,
        'event_metric': float,
        'event_status': str
    }

    def __init__(self, data):
        super().__init__(data)
        self.format = 'statstream'
        self.data = data
        self.event_name = data.get('event_name')
        self.event_metric = data.get('event_metric')
        self.event_type = data.get('event_type')
        self.event_status = data.get('event_status')
        self.event_user = data.get('event_user')
        self.event_host = data.get('event_host')
        self.event_url = data.get('event_url')
        self.event_context = data.get('event_context')
        self.event_tags = data.get('event_tags')
        self.tags = EventTagCollection(
            tags=data.get('event_tags'),
            reporter_format=self.format
        )

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Statstream Event - (statstream)

        Used with the Statstream reporter. Event may be specifed as:

        {event_fields}

        Statstream event tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''


    