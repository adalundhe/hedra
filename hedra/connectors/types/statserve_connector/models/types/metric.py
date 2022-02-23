from .tag_collection import TagCollection


class Metric:

    def __init__(self, metric):

        self.name = metric.get('event_name')
        self.stat = metric.get('metric_stat', '').lower()
        self.value = metric.get('value')
        self.host = metric.get('event_host')
        self.url = metric.get('event_url')
        self.type = metric.get('event_type')

        self.tags = TagCollection(tags=metric.get('event_tags'))
        self.tags.append_tag(
            tag_name='event_context',
            tag_value=metric.get('event_context')
        )

    def __str__(self):
        return str(self.metric_value)

    def __repr__(self):
        return self.metric_value

    def to_dict(self):
        return {
            'metric_name': self.name,
            'metric_value': self.value,
            'metric_host': self.host,
            'metric_url': self.url,
            'metric_type': self.type,
            'metric_stat': self.stat,
            'metric_tags': self.tags.to_dict_list()
        } 