import datadog
import os
from .statement import Statement


class Connection:

    def __init__(self, config):

        self._datadog_api_key =  config.get(
            'datadog_api_key',
            os.getenv('DD_API_KEY')
        )
        self._datadog_app_key = config.get(
            'datadog_app_key',
            os.getenv('DD_APP_KEY')
        )

        
        self._datadog_statsd_host = config.get(
            'datadog_statsd_host',
            os.getenv('DD_AGENT_HOST', '127.0.0.1')
        )
        self._datadog_statsd_port = config.get(
            'datadog_statsd_port',
            os.getenv('DD_DOGSTATSD_SOCKET', 8125)
        )

        self.responses = []

    async def connect(self) -> None:

        config = {
            'statsd_host': self._datadog_statsd_host,
            'statsd_port': self._datadog_statsd_port,
            'api_key': self._datadog_api_key,
            'app_key': self._datadog_app_key

        }

        datadog.initialize(**config, hostname_from_config=False)


    async def execute(self, query) -> None:
        statement = Statement(query)
        response = await statement.execute()

        self.responses += [response]

    async def commit(self) -> list:
        responses = self.responses
        self.responses = []

        return responses

    async def clear(self) -> None:
        self.responses = []

    async def close(self) -> None:
        pass


        
        
