import datetime


def event_to_prometheus_metric(event, session_id):

    return {
        'name': event.event.name,
        'type': event.event.type,
        'value': event.event.value,
        'description': 'Event of {event_name} - {event_type} for {session_id} at {time}'.format(
            event_name=event.event.name,
            event_type=event.event.type,
            session_id=session_id,
            time=event.get_utc_time()
        ),
        'labels': {
            'session_id': session_id,
            'event_status': event.event.status,
            **event.event.labels
        },
        'options': {
            'update_type': event.event.subtype
        }
    }

def metric_to_prometheus_metric(metric, session_id):
    return {
        'name': metric.metric.metric_name,
        'type': metric.metric.metric_type,
        'value': metric.metric.metric_value,
        'description': 'Response metric of {metric_name} for {session_id} at {time}'.format(
            metric_name=metric.metric.metric_name,
            session_id=session_id,
            time=metric.get_utc_time()
        ),
        'labels': {
            'session_id': session_id,
            **metric.metric.metric_tags
        },
        'options': {}
    }