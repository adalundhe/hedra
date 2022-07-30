import asyncio
import functools
import re
from typing import List

from attr import fields
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup

try:
    from prometheus_client.exposition import basic_auth_handler
    from prometheus_client import (
        CollectorRegistry,
        push_to_gateway,
    )
    from .prometheus_metric import PrometheusMetric
    from prometheus_client.core import REGISTRY
    from .prometheus_config import PrometheusConfig
    has_connector = True

except ImportError:
    PrometheusConfig = None
    has_connector = False


class Prometheus:

    def __init__(self, config: PrometheusConfig) -> None:
        self.pushgateway_address = config.pushgateway_address
        self.auth_request_method = config.auth_request_method
        self.auth_request_timeout = config.auth_request_timeout
        self.auth_request_data = config.auth_request_data
        self.username = config.username
        self.password = config.password
        self.namespace = config.namespace
        self.job_name = config.job_name

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
            'maximum': 'gauge'
        }

        self._events = {}
        self._metrics = {}
        self._errors = {}
        
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
    
    async def _submit_to_pushgateway(self):
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

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:
            
            record = {
                f'{event.name}_time': event.time,
                f'{event.name}_success': 1 if event.success else 0,
                f'{event.name}_failed': 1 if event.success is False else 0
            }

            if self._events.get(event.name) is None:

                fields = {}

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

                    metric.create_metric()

                    fields[event_field] = metric

                self._events[event.name] = fields
                
            for event_field, event_value in record.items():
                if event_value and event_field in self.types_map:
                    self._events[event.name][event_field].update(event_value)

            await self._submit_to_pushgateway()

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            timings_groups_metrics = self._metrics.get(metrics_group.name)
            if timings_groups_metrics is None:
                timings_groups_metrics = {}

                for timings_group_name, timings_group in metrics_group.groups.items():
                    timings_group_metrics = timings_groups_metrics.get(timings_group_name)

                    if timings_group_metrics is None:

                        for quantile_name in metrics_group.quantiles.keys():
                            self.types_map[quantile_name] = 'gauge'

                        fields = {}

                        types_map = { **self.types_map, **metrics_group.custom_schemas}

                        for metric_field in metrics_group.stats_fields:
                            metric_type = types_map.get(metric_field)
                            metric_name = f'{metrics_group.name}_{metric_field}'.replace('.', '_')

                            tags = [
                                f'{tag.name}:{tag.value}' for tag in metrics_group.tags
                            ]

                            prometheus_metric = PrometheusMetric(
                                metric_name,
                                metric_type,
                                metric_description=f'{metrics_group.name} {metric_field}',
                                metric_labels=tags,
                                metric_namespace=self.namespace,
                                registry=self.registry
                            )
                            prometheus_metric.create_metric()

                            fields[metric_field] = prometheus_metric

                        timings_groups_metrics[timings_group_name] = fields

                self._metrics[metrics_group.name] = timings_groups_metrics
                

            timings_groups_metrics = self._metrics.get(metrics_group.name)
            for timings_group_name, timings_group in metrics_group.groups.items():

                timings_group_metrics = timings_groups_metrics.get(timings_group_name)

                record = timings_group.record
                for field in timings_group_metrics[timings_group_name]:
                    metric_value = record.get(field)
                    field_metric = timings_group_metrics.get(field)
                    field_metric.update(metric_value)
        
        await self._submit_to_pushgateway()
    
    async def submit_events(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            if self._errors.get(metrics_group.name) is None:
                errors_metric = PrometheusMetric(
                    f'{metrics_group.name}_errors',
                    'count',
                    metric_description=f'Errors for action - {metrics_group.name}.',
                    metric_labels=[],
                    metric_namespace=self.namespace,
                    registry=self.registry
                )

                errors_metric.create_metric()

                self._errors[metrics_group.name] = errors_metric

            for error in metrics_group.errors:
                
                errors_metric.update(
                    error.get('count'),
                    labels={
                        'message': error.get('message')
                    }
                )
                
        await self._submit_to_pushgateway()

    async def close(self):
        pass
                