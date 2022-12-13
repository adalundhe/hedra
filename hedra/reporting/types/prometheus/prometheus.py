import asyncio
import functools
from typing import List
from numpy import float32, float64, int16, int32, int64
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

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

except Exception:
    PrometheusConfig = None
    has_connector = False
    basic_auth_handler = lambda: None


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
            'actions_per_second': 'gauge',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge'
        }

        self._events = {}
        self._metrics = {}
        self._stage_metrics = {}
        self._custom_metrics = {}
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

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        
        for metrics_set in metrics_sets:

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]
            
            stage_metrics = self._stage_metrics.get(metrics_set.name)
            if stage_metrics is None:
                stage_metrics = {}

                for field in metrics_set.common_stats.keys():

                    metric_name = f'{metrics_set.name}_{field}'.replace('.', '_')
                    metric_type = self.types_map.get(field)

                    prometheus_metric = PrometheusMetric(
                        metric_name,
                        metric_type,
                        metric_description=f'{metrics_set.name} {field}',
                        metric_labels=[
                            *tags,
                            f'stage:{metrics_set.stage}',
                            'group:common'
                        ],
                        metric_namespace=self.namespace,
                        registry=self.registry
                    )
                    prometheus_metric.create_metric()
                    
                    stage_metrics[field] = prometheus_metric
                
                self._stage_metrics[metrics_set.name] = stage_metrics

            self._stage_metrics[metrics_set.name] = stage_metrics
            for field, value in metrics_set.common_stats.items():
                metric = stage_metrics.get(field)
                metric.update(value)

        await self._submit_to_pushgateway()

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]

            stage_metrics = self._metrics.get(metrics_set.name)
            if stage_metrics is None:
                stage_metrics = {}

                for group_name, group in metrics_set.groups.items():
                    group_metrics = stage_metrics.get(group_name)

                    if group_metrics is None:
                        group_metrics = {}

                        for quantile_name in metrics_set.quantiles:
                            self.types_map[quantile_name] = 'gauge'

                        fields = {}

                        for metric_field in metrics_set.stats_fields:
                            metric_type = self.types_map.get(metric_field)
                            metric_name = f'{metrics_set.name}_{group_name}_{metric_field}'.replace('.', '_')

                            prometheus_metric = PrometheusMetric(
                                metric_name,
                                metric_type,
                                metric_description=f'{metrics_set.name} {metric_field}',
                                metric_labels=[
                                    *tags,
                                    f'stage:{metrics_set.stage}',
                                    f'group:{group_name}'
                                ],
                                metric_namespace=self.namespace,
                                registry=self.registry
                            )
                            prometheus_metric.create_metric()

                            fields[metric_field] = prometheus_metric

                        group_metrics[group_name] = fields

                stage_metrics[metrics_set.name] = group_metrics

            self._metrics[metrics_set.name] = stage_metrics
            for group_name, group in metrics_set.groups.items():
                group_metrics = stage_metrics.get(group_name)
                record = group.record

                for field in group_metrics[group_name]:
                    metric_value = record.get(field)
                    field_metric = group_metrics.get(field)
                    field_metric.update(metric_value)

        await self._submit_to_pushgateway()

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:

            stage_metrics = self._custom_metrics.get(metrics_set.name)
            if stage_metrics is None:
                stage_metrics = {}

                for custom_group_name, group in metrics_set.custom_metrics.items():
                    group_metrics = stage_metrics.get(custom_group_name)

                    if group_metrics is None:

                        for quantile_name in metrics_set.quantiles.keys():
                            self.types_map[quantile_name] = 'gauge'

                        fields = {}

                        for field, value in group.items():
                            
                            metric_type = None
                            if isinstance(value, (int, int16, int32, int64)):
                                metric_type = 'count'

                            elif isinstance(value, (float, float32, float64)):
                                metric_type = 'gauge'

                            metric_name = f'{metrics_set.name}_{field}'.replace('.', '_')

                            tags = [
                                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
                            ]

                            prometheus_metric = PrometheusMetric(
                                metric_name,
                                metric_type,
                                metric_description=f'{metrics_set.name} {field}',
                                metric_labels=[
                                    *tags,
                                    f'stage:{metrics_set.stage}',
                                    f'group:{custom_group_name}'
                                ],
                                metric_namespace=self.namespace,
                                registry=self.registry
                            )
                            prometheus_metric.create_metric()

                            fields[field] = prometheus_metric

                        group_metrics[custom_group_name] = fields

                stage_metrics[metrics_set.name] = group_metrics

            self._custom_metrics[metrics_set.name] = stage_metrics
            for group_name, group in metrics_set.custom_metrics.items():
                group_metrics = stage_metrics.get(group_name)

                for field in group_metrics[group_name]:
                    metric_value = group.get(field)
                    field_metric = group_metrics.get(field)
                    field_metric.update(metric_value)

        await self._submit_to_pushgateway()
                    
    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:

            if self._errors.get(metrics_set.name) is None:
                errors_metric = PrometheusMetric(
                    f'{metrics_set.name}_errors',
                    'count',
                    metric_description=f'Errors for action - {metrics_set.name}.',
                    metric_labels=[],
                    metric_namespace=self.namespace,
                    registry=self.registry
                )

                errors_metric.create_metric()
                self._errors[metrics_set.name] = errors_metric

            for error in metrics_set.errors:              
                errors_metric.update(
                    error.get('count'),
                    labels={
                        'message': error.get('message')
                    }
                )
                
        await self._submit_to_pushgateway()

    async def close(self):
        pass
                