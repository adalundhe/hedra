from .statstream_event import StatStreamEvent

class StatServeEvent(StatStreamEvent):
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
        self.format = 'statserve'
        self.tags.update_format(self.format)

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Statserve Event - (statserve)

        Used with the Statserve reporter. Event may be specifed as:

        {event_fields}

        Statserve event tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''
