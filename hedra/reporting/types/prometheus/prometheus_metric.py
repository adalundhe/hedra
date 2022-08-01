from typing import Any, List
try:
    from prometheus_client import (
        Info,
        Summary,
        Counter,
        Gauge,
        Histogram,
        Enum
    )

except Exception:
    Info = None
    Summary = None
    Counter = None
    Gauge = None
    Histogram = None
    Enum = None
    pass


class PrometheusMetric:

    types = {
            'info': Info,
            'summary': Summary,
            'count': Counter,
            'gauge': Gauge,
            'histogram': Histogram,
            'enum': Enum
        }

    def __init__(
        self,
        metric_name: str, 
        metric_type: str, 
        metric_description: str=None,
        metric_states: List[Any]= None,
        metric_labels: List[str]=[],
        metric_namespace: str = None,
        registry=None
    ):
        self.name = metric_name
        self.type = metric_type
        self.description = metric_description
        self.states = metric_states
        self.label_names = metric_labels
        self.namespace = metric_namespace
        self.registry = registry
        self.prometheus_object = None

    def create_metric(self):
        
        if self.type == 'enum':
            self.prometheus_object = Enum(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                states=self.states,
                registry=self.registry,
                namespace=self.namespace
            )

        elif self.type == 'gauge':
            self.prometheus_object = Gauge(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=self.registry,
                namespace=self.namespace
            )

        elif self.type == 'summary':
            self.prometheus_object = Summary(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=self.registry,
                namespace=self.namespace
            )

        elif self.type == 'count':
            self.prometheus_object = Counter(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=self.registry,
                namespace=self.namespace
            )

        elif self.type == 'histogram':
            self.prometheus_object = Histogram(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=self.registry,
                namespace=self.namespace
            )

        elif self.type == 'info':
            self.prometheus_object = Info(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=self.registry,
                namespace=self.namespace
            )

        self.prometheus_object._labelnames = self.label_names

        self.generators = {
            'info': self._update_info,
            'enum': self._update_enum,
            'histogram': self._update_histogram_or_summary,
            'summary': self._update_histogram_or_summary,
            'count': self._update_count,
            'gauge': self._update_gauge,
        }

    def update(self, value=None, labels=None, options=None) -> None:
        if labels is None:
            labels = {label: '' for label in self.label_names}

            self.prometheus_object.labels(**labels)

        self.generators.get(self.type)(value, labels, options)

    def _update_info(self, value, labels, options) -> None:
        if value is None:
            value = 'N/A'

        self.prometheus_object.info(value)

    def _update_enum(self, value, labels, options) -> None:
        if value is None:
            value = 'N/A'

        self.prometheus_object.state(value)

    def _update_histogram_or_summary(self, value, labels, options) -> None:
        if value is None:
            value = 0

        self.prometheus_object.observe(value)

    def _update_count(self, value, labels, options) -> None:
        self.prometheus_object.inc()

    def _update_gauge(self, value, labels, options) -> None:
        if value is None:
            value = 1

        if options is None:
            options = {}

        if options.get('update_type') == 'inc':
            self.prometheus_object.inc(amount=value)
        elif options.get('update_type') == 'dec':
            self.prometheus_object.dec(amount=value)
        elif options.get('update_type') == 'set_function':
            self.prometheus_object.set_function(
                options.get('update_function')(value)
            )
        else:
            self.prometheus_object.set(value)
