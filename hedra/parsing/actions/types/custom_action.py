class CustomAction:

    def __init__(self, action, group=None, engine=None) -> None:
        
        self.name = action.get('name')
        self.user = action.get('user')
        self.env = action.get('env')
        self.action = action.get('action')
        self.weight = action.get('weight')
        self.order = action.get('order')
        self.tags = action.get('tags')
        self.url = action.get('url')
        self.type = action.get('type')
        self.timeout = action.get('timeout')
        self.wait_interval = action.get('wait_interval', 0)
        self.success_condition = action.get('success_condition')
        self.action_type = 'custom'
        self.is_setup = self.action.is_setup
        self.is_teardown = self.action.is_teardown
        self.group = group
        self.engine = engine

        if self.type is None:
            self.type = self.action_type

        if self.tags is None:
            self.tags = []

        if self.group is None:
            self.group = self.user

    @classmethod
    def about(cls):
        return '''
        Custom Action

        Custom actions are used by Hedra's Action Set engine and represet a single
        class-method action of any valid Hedra test class inheriting from the Hedra
        Action Set base clase. For example:

        @action('my_action')
        async def my_action(self):
            yield self.session.get('https://www.google.com')

        Actions are provided as Python code. For more information on how to use
        Hedra's Action Sets and hooks to write performance tests as code, run the
        command:

            hedra --about testing

        '''

    def execute(self, context):
        return self.action()
        
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'user': self.user,
            'tags': self.tags,
            'env': self.env,
            'url': self.url,
            'type': self.type,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type,
            'success_condition': self.success_condition
        }