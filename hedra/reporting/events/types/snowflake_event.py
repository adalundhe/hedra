from .statstream_event import StatStreamEvent


class SnowflakeEvent(StatStreamEvent):
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
        super(SnowflakeEvent, self).__init__(data)
        self.format = 'snowflake'
        self.tags.update_format(self.format)

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Snowflake Event - (snowflake)

        Used with the Snowflake reporter. Events may be specifed as:

        {event_fields}

        Snowflake events tags are submitted as a list of Snowflake records to
        their own table with:

        - tag_name: name of the tag
        - tag_value: tag value

        as table fields.
        
        '''