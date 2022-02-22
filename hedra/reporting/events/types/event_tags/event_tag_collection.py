from .event_tag import EventTag


class EventTagCollection:

    def __init__(self, tags=None, reporter_format=None):
        self.format = reporter_format
        if tags is None:
            tags = []

        self.tags = []

        for tag in tags:
            self.tags.append(
                EventTag(
                    reporter_format=self.format,
                    tag_name=tag.get('tag_name'),
                    tag_value=tag.get('tag_value')
                )
            )

    def __iter__(self):
        for tag in self.tags:
            yield tag

    def to_string_list(self):
        return [str(tag) for tag in self.tags]

    def to_dict_list(self):
        return [tag.to_dict() for tag in self.tags]

    def to_dict(self):
        return {tag.name: tag.value for tag in self.tags}

    def update_format(self, reporter_format=None):
        for tag in self.tags:
            tag.format = reporter_format
