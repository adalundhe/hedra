import datadog
from async_tools.functions.awaitable import awaitable


class Monitor:

    def __init__(self, monitor):
        self._monitor = monitor
        self.type = monitor.get('type')
        self.id = monitor.get('id')
        self.name = monitor.get('name')
        self.priority = monitor.get('priority', 3)
        self.query = monitor.get('query')
        self.roles = monitor.get('roles', [])
        self.tags = monitor.get('tags', [])
        self.message = monitor.get('message')
        self.monitor_type = monitor.get('monitor_type')
        self.options = monitor.get('options', {})

    async def get(self) -> dict:
        if self.options.get('all'):
            response = await awaitable(datadog.api.Monitor.get_all)

        elif self.options.get('search'):
            response = await awaitable(datadog.api.Monitor.search, query=self.name)

        elif self.options.get('search_groups'):
            response = await awaitable(datadog.api.Monitor.search_groups, query=self.name)

        else:
            response = await awaitable(datadog.api.Monitor.get, self.id)

        return response

    async def create(self) -> dict:
        update = await self._assemble_monitor_request()
        response = datadog.api.Monitor.create(
            id=self.id,
            body=update
        )

        return response

    async def update(self) -> dict:
        
        if self.options.get('mute_all'):
            response = await awaitable(datadog.api.Monitor.mute_all)
        
        elif self.options.get('mute'):
            response = await awaitable(datadog.api.Monitor.mute, self.id)

        else:
            update = await self._assemble_monitor_request()
            response = await awaitable(
                datadog.api.Monitor.update,
                id=self.id,
                body=update
            )

        return response

    async def delete(self) -> dict:
        return await awaitable(datadog.api.Monitor.delete, self.id)

    async def _assemble_monitor_request(self) -> dict:
        return {
            'name': self.name,
            'priority': self.priority,
            'type': self.monitor_type,
            'query': self.query,
            'tags': self.tags,
            'restricted_roles': self.roles,
            'message': self.message,
            'options': self.options
        }
