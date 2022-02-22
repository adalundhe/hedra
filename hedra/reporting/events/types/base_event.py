class BaseEvent:
    fields = {}

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return {field: getattr(self, field) for field in self.fields}