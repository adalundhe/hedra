from .base_event import BaseEvent
from .event_tags import EventTagCollection
from .postgres_event import PostgresEvent


class CassandraEvent(PostgresEvent):
    fields = {
        'event_name': str,
        'event_metric': float,
        'event_type': str,
        'event_status': str,
        'event_user': str,
        'event_host': str,
        'event_url': str,
        'event_context': str
    }

    def __init__(self, data):
        super().__init__(data)
        self.format = 'cassandra'
        self.tags.update_format(reporter_format=self.format)

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Cassandra Event - (cassandra)

        Used with the Cassandra reporter. Events may be specifed as:

        {event_fields}

        Cassandra event tags are submitted as a list of Cassandra records to
        their own table with:

        - tag_name: name of the tag
        - tag_value: tag value

        as table fields.
        
        '''

       
        