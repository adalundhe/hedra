from .statstream_event import StatStreamEvent


class PostgresEvent(StatStreamEvent):
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
        super(PostgresEvent, self).__init__(data)
        self.format = 'postgres'
        self.tags.update_format(reporter_format='postgres')

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Postgres Event - (postgres)

        Used with the Postgres reporter. Event may be specifed as:

        {event_fields}

        Postgres event tags are submitted as a list of Postgres records to
        their own table with:

        - tag_name: name of the tag
        - tag_value: tag value

        as table fields.
        
        '''