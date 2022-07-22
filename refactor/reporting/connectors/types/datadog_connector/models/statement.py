from .types import (
    Event,
    Metric,
    Monitor,
    Dashboard
)


class Statement:

    def __init__(self, query):
        self.query = query
        self._type = query.get('type')
        self._action = query.get('action', 'get')
        self.response = None
        
        datadog_types = {
            'event': Event,
            'metric': Metric,
            'monitor': Monitor,
            'dashboard': Dashboard
        }

        self._datadog_type = datadog_types.get(
            self._type
        )(query)

    async def execute(self):
        
        if self._action == 'create':
            return await self._datadog_type.create()

        elif self._action == 'update':
            return await self._datadog_type.update()

        elif self._action == 'delete':
            return await self._datadog_type.delete()

        else:
            return await self._datadog_type.get()


    
