class Tag:

    def __init__(self, tag_name=None, tag_value=None):
        self.name = tag_name
        self.value = tag_value

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