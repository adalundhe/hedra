import uuid

def to_redis_event(session_id, event):
    return {
        'key': 'event_{session_id}_{event_name}_{event_id}'.format(
            session_id=session_id,
            event_name=event.event.name,
            event_id=uuid.uuid4()
        ),
        'type': 'set',
        'value': event.event.value,
        'options': {
            'serialize': True
        }
    }


def to_redis_metric(session_id, metric):
    return {
        'key': 'metric_{session_id}_{metric_name}'.format(
            session_id=session_id,
            metric_name=metric.metric.metric_name
        ),
        'type': 'set',
        'value': metric.to_dict(),
        'options': {
            'serialize': True
        }
    }


def to_redis_event_query(session_id, key):
    return {
        'key': 'event_{session_id}_{key}_*'.format(
            session_id=session_id,
            key=key
        )
    }