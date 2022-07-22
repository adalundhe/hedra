from .metric_tag import MetricTag


class MetricTagCollection:

    def __init__(self, tags=None, parsed=False, reporter_format=None):
        self.format = reporter_format
        if tags is None:
            tags = []

        self.tags = []

        if parsed:
             for tag in tags:
                self.tags.append(
                    MetricTag(
                        tag_name=tag.tag_name,
                        tag_value=tag.tag_value
                    )
                )

        else:
            for tag in tags:
                self.tags.append(
                    MetricTag(
                        metric_tag=tag
                    )
                )

    def __iter__(self):
        for tag in self.tags:
            yield tag

    def add_tag(self, tag, parsed=False):
        if parsed:
            self.tags.append(
                MetricTag(
                    tag_name=tag.tag_name,
                    tag_value=tag.tag_value
                )
            )

        if type(tag) is dict:
            self.tags.append(
                MetricTag(
                    tag_name=tag.get('name'),
                    tag_value=tag.get('value')
                )
            )
        
        else:
            self.tags.append(
                MetricTag(metric_tag=tag)
            )

    def add_tags(self, tags_list, parsed=False):
        for tag in tags_list:
            self.add_tag(tag, parsed=parsed)

    def to_string_list(self):
        return [str(tag) for tag in self.tags]

    def to_dict_list(self):
        return [tag.to_dict() for tag in self.tags]

    def to_dict(self):
        return {tag.name: tag.value for tag in self.tags}
    
    def update_format(self, reporter_format=None):
        for tag in self.tags:
            tag.format = reporter_format
