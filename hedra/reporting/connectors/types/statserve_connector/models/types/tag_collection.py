from .tag import Tag


class TagCollection:

    def __init__(self, tags=None, connector_format=None):
        if tags is None:
            tags = []

        self.tags = []

        for tag in tags:
            self.tags.append(
                Tag(
                    tag_name=tag.get('tag_name'),
                    tag_value=tag.get('tag_value')
                )
            )

    def __iter__(self):
        for tag in self.tags:
            yield tag

    async def __aiter__(self):
        for tag in self.tags:
            yield tag

    def append_tag(self, tag_name=None, tag_value=None):
        self.tags.append(
            Tag(
                tag_name=tag_name,
                tag_value=tag_value
            )
        )

    def to_string_list(self):
        return [str(tag) for tag in self.tags]

    def to_dict_list(self):
        return [tag.to_dict() for tag in self.tags]

    def to_dict(self):
        return {tag.name: tag.value for tag in self.tags}