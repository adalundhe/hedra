import datetime
import time
from .base_event import BaseEvent
from .event_tags import EventTagCollection


class PrometheusEvent(BaseEvent):
    fields = {
        'name': str,
        'value': float,
        'type': str,
        'status': str,
        'labels': str
    }

    def __init__(self, data):
        super(PrometheusEvent, self).__init__(data)
        
        self.format = 'prometheus'
        self.name = data.get('event_name')
        self.value = data.get('event_metric')
        self.status = data.get('event_status')

        event_type_context = data.get('event_type').split(':')
        self.type = event_type_context[0]

        if len(event_type_context) > 1:
            self.subtype = event_type_context[1]
        else:
            self.subtype = None

        self.tags = EventTagCollection(
            tags=data.get('event_tags'),
            reporter_format=self.format
        )
        
        self.labels = self.tags.to_dict()
        self.labels.update({
            'event_context': data.get('event_context'),
            'event_user': data.get('event_user'),
            'event_host': data.get('event_host'),
            'event_url': data.get('event_url')
        })

    @classmethod
    def about(cls):
        event_fields = '\n\t'.join([f'- {field}' for field in cls.fields])

        return f'''
        Prometheus Event - (prometheus)

        Used with the Prometheus reporter. Event may be specifed as:

        {event_fields}

        The Prometheus event will automatically map any non-count statistics
        to Prometheus Gauge events and will apply any non-metric information
        (event url, event host, event type, tags, etc.) to labels for that
        event.
        
        '''


        