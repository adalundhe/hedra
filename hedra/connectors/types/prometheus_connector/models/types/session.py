import os
from prometheus_api_client import PrometheusConnect
from async_tools.functions.awaitable import awaitable
from .response import Response

class Session:

    def __init__(self, config):
        self.metric_store = {}
        self.results_store = {}

        self.prometheus_host_type = config.get(
            'prometheus_host_type',
            os.getenv('PROMETHEUS_HOST_TYPE', 'http')
        )

        self.prometheus_host = config.get(
            'prometheus_client_host',
            os.getenv('PROMETHEUS_CLIENT_HOST', 'localhost')
        )

        self.prometheus_port = config.get(
            'prometheus_client_port',
            os.getenv('PROMETHEUS_CLIENT_PORT', 9090)
        )

        self.prometheus_address = '{prometheus_host_type}://{prometheus_host}:{prometheus_port}'.format(
            prometheus_host_type=self.prometheus_host_type,
            prometheus_host=self.prometheus_host,
            prometheus_port=self.prometheus_port
        )

        self.prometheus_api_connection = PrometheusConnect(
            url=self.prometheus_address,
            headers=config.get('prometheus_connection_headers'),
            disable_ssl=config.get('prometheus_use_ssl', False)
        )

        self.current_statement = None

    async def update_metrics(self, statement, registry=None):
        if statement.metric.name in self.metric_store:
            prometheus_metric = self.metric_store.get(statement.metric.name)

        else:
            prometheus_metric = await statement.as_prometheus_metric(registry=registry)

        await prometheus_metric.update(statement.metric.value, labels=statement.metric.labels)
        self.metric_store[statement.metric.name] = prometheus_metric


    async def query_metrics(self, statement, registry=None) -> None:

        if self.results_store.get(statement.metric.name) is None:
            self.results_store[statement.metric.name] = []

        prometheus_query = await statement.as_prometheus_query()

        if prometheus_query.type == 'value':
            query_dict = await prometheus_query.assemble_value_query()
            raw_response = await awaitable(self.prometheus_api_connection.get_current_metric_value, **query_dict
            )
            response = Response(raw_response)
            await response.format_value_response()

        elif prometheus_query.type == 'range':
            query_dict = await prometheus_query.assemble_range_query()
            raw_response = await awaitable(self.prometheus_api_connection.get_metric_range_data, **query_dict)
            response = Response(raw_response)
            await response.format_range_response()

        elif prometheus_query.type == 'aggregation':


            if statement.metric.name in self.metric_store:
                prometheus_metric = self.metric_store.get(statement.metric.name)

            else:
                prometheus_metric = await statement.as_prometheus_metric(registry=registry)

            query_dict = await prometheus_query.assemble_aggregation_query()
            raw_response = await awaitable(self.prometheus_api_connection.get_metric_aggregation, **query_dict)
            response = Response(raw_response, metric=prometheus_metric)
            await response.format_aggregation_response()

        elif prometheus_query.type == 'custom_range':
            query_dict = await prometheus_query.assemble_custom_range_query()
            raw_response = await awaitable(self.prometheus_api_connection.custom_query_range, **query_dict)
            response = Response(raw_response)
            
            if prometheus_query.options.get('time_range'):
                await response.format_range_response()
            else:
                await response.format_value_response()

        else:
            query_dict = await prometheus_query.assemble_custom_query()
            raw_response = await awaitable(self.prometheus_api_connection.custom_query, **query_dict)
            response = Response(raw_response)

            if prometheus_query.options.get('time_range'):
                await response.format_range_response()
            else:
                await response.format_value_response()

        self.results_store.update(response.cleaned_data)


    async def clear_local_results_store(self):
        for prometheus_metric_name in self.results_store:
            self.results_store[prometheus_metric_name] = []

    async def clear_local_statement_store(self):
        for prometheus_metric_name, prometheus_metric in self.metric_store.items():
            self.metric_store[prometheus_metric_name] = None

        


    

