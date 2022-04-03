class Metric:

    def __init__(self, data):
        self.name = data.get('name')
        self.type = data.get('type')
        self.description = data.get('description')
        self.value = data.get('value')
        self.labels = data.get('labels', {})
        self.states = data.get('states')
        self.instance = data.get('instance')
        self.namespace = data.get('namespace')
        self.operators = data.get('operators')
        self.options = data.get('options', {})