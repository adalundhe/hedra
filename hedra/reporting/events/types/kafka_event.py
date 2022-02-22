import json
from .statstream_event import StatStreamEvent
from .event_tags import EventTagCollection


class KafkaEvent(StatStreamEvent):
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
        super(KafkaEvent, self).__init__(data)

    def to_format(self):
        return {
            'message_value': {
                'event_value': self.event_name,
                'event_metric': self.event_metric,
                'event_type': self.event_type,
                'event_status': self.event_status,
                'event_user': self.event_user,
                'event_host': self.event_host,
                'event_url': self.event_url,
                'event_context': self.event_context,
                'event_tags': self.tags.to_dict_list()
            }
        }

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Kafka Event - (prometheus)

        Used with the Kafka reporter. Event may be specifed as:

        {event_fields}

        Kafka events are submitted Kafka messages to the specified events
        topic as serialized JSON, with tags submitted as a list of dictionaries 
        with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''
