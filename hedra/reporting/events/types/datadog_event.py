import datetime
import time
from .base_event import BaseEvent
from .event_tags import EventTagCollection


class DataDogEvent(BaseEvent):
    fields = {
        'host': str,
        'text': str,
        'title': str,
        'aggregation_key': str,
        'priority': str,
        'source': str,
        'alert_type': str,
        'tags': list
    }

    def __init__(self, data):
        super().__init__(data)
        self.data = data
        self.format = 'datadog'

        alert_type_map = {
            'SUCCESS': 'success',
            'FAILURE': 'error'
        }

        self.host = data.get('event_host')
        self.text = 'EVENT METRIC: {event_metric}'.format(
            event_metric=data.get('event_metric')
        )
        self.title = "{event_type} {event_name}".format(
            event_type=data.get('event_type'),
            event_name=data.get('event_name')
        )
        self.aggregation_key = "{event_type}_{event_name}".format(
            event_type=data.get('event_type'),
            event_name=data.get('event_name')
        )

        if data.get('event_user'):
            self.aggregation_key += "_{event_user}".format(
                user=data.get('user')
            )

        self.priority = 'normal'
        self.source = data.get('event_url')
        self.alert_type = alert_type_map.get(
            data.get('event_status'), 'INFO'
        )

        tags = EventTagCollection(
            tags=data.get('event_tags'),
            reporter_format=self.format
        )
        self.tags = tags.to_dict_list()

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Datadog Event - (datadog)

        Used with the Datadog reporter. Event may be specifed as:

        {event_fields}

        Note that the structure of Datadog events differs from other event
        types. Fields are mapped as follows:

        - <event_host> -> <host>

        - <event_metric> -> <text>

        - <event_type>_<event_name> -> <title>

        - <event_type>_<event_name>_<event_user> -> <aggregation_key> (Note that if no event user is provided only event_type and event_name will be used)

        - <event_url> -> <source>

        - <event_status> -> <alert_type> (default is INFO, success maps to SUCCESS and failure maps to FAILURE)

        Datadog event tags are submitted as a list of dictionaries with:

        - tag_name: name of the tag
        - tag_value: tag value

        as content.
        
        '''
