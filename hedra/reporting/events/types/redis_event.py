import pickle
from .base_event import BaseEvent
from .event_tags import EventTagCollection


class RedisEvent(BaseEvent):
    fields = {
        'key': str,
        'value': str
    }

    def __init__(self, data):
        super(RedisEvent, self).__init__(data)
        self.format = 'redis'
        self.key = data.get('event_name')
        self.value = data

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Redis Event - (prometheus)

        Used with the Redis reporter. Event may be specifed as:

        {event_fields}

        Redis events are submitted Redis key/value pairs with the key specified as:

            <event_name>
        
        The value is stored as serialized JSON, with tags submitted as a list of 
        dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''