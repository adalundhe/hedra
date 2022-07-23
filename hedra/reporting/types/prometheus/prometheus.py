import asyncio
import functools
from typing import Any, List
try:
    from prometheus_client.exposition import basic_auth_handler
    from prometheus_client import (
        CollectorRegistry,
        push_to_gateway,
    )
    from .prometheus_metric import PrometheusMetric
    from prometheus_client.core import REGISTRY
    has_connector = True

except ImportError:
    has_connector = False


class Prometheus:

    def __init__(self, config: Any) -> None:
        self.pushgateway_address = config.pushgateway_address
        self.auth_request_method = config.auth_request_method or 'GET'
        self.auth_request_timeout = config.auth_request_timeout or 60000
        self.auth_request_data = config.auth_request_data
        self.username = config.username
        self.password = config.password
        self.namespace = config.namespace
        self.job_name = config.job_name
        self.custom_fields = config.custom_fields or {}

        self.registry = None
        self._auth_handler = None
        self._has_auth = False
        self._loop = asyncio.get_event_loop()
        
        self.types_map = {
            'total': 'count',
            'succeeded': 'count',
            'failed': 'count',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge',
            'quantiles': 'gauge',
            **self.custom_fields
        }

        self._events = {}
        self._metrics = {}
        
    async def connect(self) -> None:
        self.registry = CollectorRegistry()
        REGISTRY.register(self.registry)

        if self.username and self.password:
            self._has_auth = True
            

    def _generate_auth(self) -> basic_auth_handler:
        return basic_auth_handler(
            self.pushgateway_address,
            self.auth_request_method,
            self.auth_request_timeout,
            {
                'Content-Type': 'application/json'
            },
            self.auth_request_data,
            username=self.username,
            password=self.password
        )
    
    async def _submit_metrics_to_pushgateway(self):
        if self._has_auth:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    push_to_gateway,
                    self.pushgateway_address,
                    job=self.job_name,
                    registry=self.registry,
                    handler=self._generate_auth
                )
            )

        else:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    push_to_gateway,
                    self.pushgateway_address,
                    job=self.job_name,
                    registry=self.registry
                )
            )

    async def submit_events(self, events: List[Any]):

        for event in events:
            
            record = event.stats

            if self._events.get(event.name) is None:

                self._events[event.name] = {}

                for event_field in record.keys():
                    metric_type = self.types_map.get(event_field)
                
                    metric = PrometheusMetric(
                        f'{event.name}_{event_field}',
                        metric_type,
                        metric_description=f'{event.name} {event_field}',
                        metric_labels=[],
                        metric_namespace=self.namespace,
                        registry=self.registry
                    )

                    self._events[event.name][event_field] = metric
                
            for event_field, event_value in record.items():
                if event_value and event_field in self.types_map:
                    self._events[event.name][event_field].update(event_value)

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
            
            record = metric.record
            if self._metrics.get(metric.name) is None:

                self._metrics[metric.name] = {}

                for metric_field in metric.fields:
                    metric_type = self.types_map.get(metric_field)
                
                    metric = PrometheusMetric(
                        f'{metric.name}_{metric_field}',
                        metric_type,
                        metric_description=f'{metric.name} {metric_field}',
                        metric_labels=[],
                        metric_namespace=self.namespace,
                        registry=self.registry
                    )

                    self._metrics[metric.name][metric_field] = metric
                
            for metric_field, metric_value in record.items():
                if metric_value and metric_field in self.types_map:
                    self._metrics[metric.name][metric_field].update(metric_value)

    async def close(self):
        pass
                