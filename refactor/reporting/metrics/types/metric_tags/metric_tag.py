class MetricTag:

    def __init__(self, metric_tag=None, tag_name=None, tag_value=None, reporter_format=None):
        self.format = reporter_format
        if metric_tag is None:
            self.name = tag_name
            self.value = tag_value
        elif type(metric_tag) is dict:
            self.name = metric_tag.get('name')
            self.value = metric_tag.get('value')
        else:
            parsed_tag = metric_tag.split(':')
            self.name = parsed_tag[0]
            self.value = parsed_tag[1]

    def __str__(self):
        return '{tag_name}:{tag_value}'.format(
            tag_name=self.name,
            tag_value=self.value
        )
    
    def to_dict(self):
        return {
            'name': self.name,
            'value': self.value
        }
