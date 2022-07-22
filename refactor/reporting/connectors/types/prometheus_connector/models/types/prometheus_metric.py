from prometheus_client import (
    Info,
    Summary,
    Counter,
    Gauge,
    Histogram,
    Enum
)


class PrometheusMetric:

    types = {
            'info': Info,
            'summary': Summary,
            'count': Counter,
            'gauge': Gauge,
            'histogram': Histogram,
            'enum': Enum
        }

    def __init__(self, metric, registry=None):
        self.name = metric.name
        self.type = metric.type
        self.description = metric.description
        self.states = metric.states
        self.label_names = list(metric.labels.keys())
        
        if self.type == 'enum':
            self.prometheus_object = Enum(
                self.name,
                self.description,
                labelnames=self.label_names,
                states=self.states,
                registry=registry,
                namespace=metric.namespace
            )

        elif self.type == 'gauge':
            self.prometheus_object = Gauge(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=registry,
                namespace=metric.namespace
            )

        elif self.type == 'summary':
            self.prometheus_object = Summary(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=registry,
                namespace=metric.namespace
            )

        elif self.type == 'count':
            self.prometheus_object = Counter(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=registry,
                namespace=metric.namespace
            )

        elif self.type == 'histogram':
            self.prometheus_object = Histogram(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=registry,
                namespace=metric.namespace
            )

        elif self.type == 'info':
            self.prometheus_object = Info(
                self.name,
                self.description,
                labelnames=(label for label in self.label_names),
                registry=registry,
                namespace=metric.namespace
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

    async def update(self, value, labels=None, options=None) -> None:
        if labels is None:
            labels = {label: '' for label in self.label_names}

        await self.generators.get(self.type)(value, labels, options)

    async def _update_info(self, value, labels, options) -> None:
        if value is None:
            value = 'N/A'

        self.prometheus_object.labels(**labels).info(value)

    async def _update_enum(self, value, labels, options) -> None:
        if value is None:
            value = 'N/A'

        self.prometheus_object.labels(**labels).state(value)

    async def _update_histogram_or_summary(self, value, labels, options) -> None:
        if value is None:
            value = 0

        self.prometheus_object.labels(**labels).observe(value)

    async def _update_count(self, value, labels, options) -> None:
        self.prometheus_object.labels(**labels).inc()

    async def _update_gauge(self, value, labels, options) -> None:
        if value is None:
            value = 1

        if options is None:
            options = {}

        if options.get('update_type') == 'inc':
            self.prometheus_object.labels(**labels).inc(amount=value)
        elif options.get('update_type') == 'dec':
            self.prometheus_object.labels(**labels).dec(amount=value)
        elif options.get('update_type') == 'set_function':
            self.prometheus_object.labels(**labels).set_function(
                options.get('update_function')(value)
            )
        else:
            self.prometheus_object.labels(**labels).set(value)
