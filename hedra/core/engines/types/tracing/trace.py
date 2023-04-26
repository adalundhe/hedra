from __future__ import annotations
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import Optional, Dict, Union
from .tracing_types import (
    Request,
    Response,
    RequestHook,
    ResponseHook,
    TraceSignal,
    UrlFilter
)
from .url_filters import default_params_strip_filter


def skip_import(*args, **kwargs):
    pass

try:
    from opentelemetry import context as context_api
    from opentelemetry import trace
    from opentelemetry.instrumentation.utils import (
        _SUPPRESS_INSTRUMENTATION_KEY,
        http_status_to_status_code
    )
    from opentelemetry.propagate import inject
    from opentelemetry.semconv.trace import SpanAttributes
    from opentelemetry.trace import SpanKind,  get_tracer, Span
    from opentelemetry.trace.status import Status, StatusCode
    from opentelemetry.util.http import remove_url_credentials

except ImportError:
    context_api = object
    trace = object
    _SUPPRESS_INSTRUMENTATION_KEY=None
    http_status_to_status_code = skip_import
    inject = skip_import
    SpanAttributes = object
    SpanKind = object
    get_tracer = skip_import
    Span = object
    Status = object
    StatusCode = object
    remove_url_credentials = skip_import


__name__ = 'hedra'
__version__ = "0.7.12"

OpenTelemetryTracingConfig = Union[
    UrlFilter,
    RequestHook,
    ResponseHook,
    TraceSignal
]


@unstable
class Trace:
    """First-class used to trace requests launched via ClientSession objects."""

    __slots__ = (
        'tracer',
        'span',
        'token',
        'allowed_traces',
        'url_filter',
        'request_hook',
        'response_hook',
        'on_request_chunk_sent',
        'on_request_headers_sent',
        'on_response_chunk_received',
        'on_request_redirect',
        'on_request_data_sent',
        'on_response_data_received',
        'on_response_headers_received',
        'on_connection_queued_start',
        'on_connection_queued_end',
        'on_connection_create_start',
        'on_connection_create_end',
        'on_connection_reuse_connection',
        'on_dns_resolve_host_start',
        'on_dns_resolve_host_end',
        'on_dns_cache_hit',
        'on_dns_cache_miss',
        'on_task_start',
        'on_task_end',
        'on_task_error',
        '_context_active'
    )

    def __init__(
        self, 
        url_filter: Optional[UrlFilter]=None,
        request_hook: Optional[RequestHook]=None,
        response_hook: Optional[ResponseHook]=None,
        on_request_headers_sent: Optional[TraceSignal]=None,
        on_request_data_sent: Optional[TraceSignal]=None,
        on_request_chunk_sent: Optional[TraceSignal]=None,
        on_response_headers_received: Optional[TraceSignal]=None,
        on_response_data_received: Optional[TraceSignal]=None,
        on_response_chunk_received: Optional[TraceSignal]=None,
        on_request_redirect: Optional[TraceSignal]=None,
        on_connection_queued_start: Optional[TraceSignal]=None,
        on_connection_queued_end: Optional[TraceSignal]=None,
        on_connection_create_start: Optional[TraceSignal]=None,
        on_connection_create_end: Optional[TraceSignal]=None,
        on_connection_reuse_connection: Optional[TraceSignal]=None,
        on_dns_resolve_host_start: Optional[TraceSignal]=None,
        on_dns_resolve_host_end: Optional[TraceSignal]=None,
        on_dns_cache_hit: Optional[TraceSignal]=None,
        on_dns_cache_miss: Optional[TraceSignal]=None,
        on_task_start: Optional[TraceSignal]=None,
        on_task_end: Optional[TraceSignal]=None,
        on_task_error: Optional[TraceSignal]=None
    ) -> None:
    
    
        
        self.allowed_traces = [
            'on_request_chunk_sent',
            'on_request_headers_sent',
            'on_response_chunk_received',
            'on_request_redirect',
            'on_request_data_sent',
            'on_response_data_received',
            'on_response_headers_received',
            'on_connection_queued_start',
            'on_connection_queued_end',
            'on_connection_create_start',
            'on_connection_create_end',
            'on_connection_reuse_connection',
            'on_dns_resolve_host_start',
            'on_dns_resolve_host_end',
            'on_dns_cache_hit',
            'on_dns_cache_miss',
            'on_task_start',
            'on_task_end',
            'on_task_error'
        ]

        if url_filter is None:
            url_filter = default_params_strip_filter

        self.url_filter = url_filter

        self.request_hook: Union[RequestHook, None] = request_hook
        self.response_hook: Union[RequestHook, None] = response_hook

        self.on_request_headers_sent: TraceSignal = on_request_headers_sent
        self.on_request_data_sent: TraceSignal = on_request_data_sent
        self.on_request_chunk_sent: TraceSignal = on_request_chunk_sent
        self.on_request_redirect: TraceSignal = on_request_redirect
        self.on_response_headers_received = on_response_headers_received
        self.on_response_data_received = on_response_data_received
        self.on_response_chunk_received: TraceSignal = on_response_chunk_received

        self.on_connection_queued_start: TraceSignal = on_connection_queued_start
        self.on_connection_queued_end: TraceSignal = on_connection_queued_end
        self.on_connection_create_start: TraceSignal = on_connection_create_start
        self.on_connection_create_end: TraceSignal = on_connection_create_end
        self.on_connection_reuse_connection: TraceSignal = on_connection_reuse_connection

        self.on_dns_resolve_host_start: TraceSignal = on_dns_resolve_host_start
        self.on_dns_resolve_host_end: TraceSignal = on_dns_resolve_host_end
        self.on_dns_cache_hit: TraceSignal = on_dns_cache_hit
        self.on_dns_cache_miss: TraceSignal = on_dns_cache_miss

        self.on_task_start = on_task_start
        self.on_task_end = on_task_end
        self.on_task_error = on_task_error

        self.tracer = get_tracer(__name__, __version__, None)
        self.span: Span = None
        self.token: object = None
        self._context_active = False

    def trace_config_ctx(
        self,
        **kwargs: Dict[str, TraceSignal]
    ) -> Trace:
        return Trace(
            tracer=self.tracer,
            url_filter=self.url_filter,
            request_hook=self.request_hook,
            response_hook=self.response_hook,
            on_request_headers_sent=self.on_request_chunk_sent,
            on_request_data_sent=self.on_request_data_sent,
            on_request_chunk_sent=self.on_request_chunk_sent,
            on_request_redirect=self.on_request_redirect,
            on_response_headers_received=self.on_response_headers_received,
            on_response_data_received=self.on_response_data_received,
            on_response_chunk_received=self.on_response_chunk_received,
            on_connection_queued_start=self.on_connection_queued_start,
            on_connection_queued_end=self.on_connection_queued_end,
            on_connection_create_start=self.on_connection_create_start,
            on_connection_create_end=self.on_connection_create_end,
            on_connection_reuse_connection=self.on_connection_reuse_connection,
            on_dns_resolve_host_start=self.on_dns_resolve_host_start,
            on_dns_resolve_host_end=self.on_dns_resolve_host_end,
            on_dns_cache_hit=self.on_dns_cache_hit,
            on_dns_cache_miss=self.on_dns_cache_miss,
            on_task_start=self.on_task_start,
            on_task_end=self.on_task_end,
            on_task_error=self.on_task_error,
            **kwargs
        )
    
    def to_dict(self) -> Dict[str, OpenTelemetryTracingConfig]:
        return {
            'url_filter': self.url_filter,
            'request_hook': self.request_hook,
            'response_hook': self.response_hook,
            'on_request_headers_sent': self.on_request_chunk_sent,
            'on_request_data_sent': self.on_request_data_sent,
            'on_request_chunk_sent': self.on_request_chunk_sent,
            'on_request_redirect': self.on_request_redirect,
            'on_response_headers_received': self.on_response_headers_received,
            'on_response_data_received': self.on_response_data_received,
            'on_response_chunk_received': self.on_response_chunk_received,
            'on_connection_queued_start': self.on_connection_queued_start,
            'on_connection_queued_end': self.on_connection_queued_end,
            'on_connection_create_start': self.on_connection_create_start,
            'on_connection_create_end': self.on_connection_create_end,
            'on_connection_reuse_connection': self.on_connection_reuse_connection,
            'on_dns_resolve_host_start': self.on_dns_resolve_host_start,
            'on_dns_resolve_host_end': self.on_dns_resolve_host_end,
            'on_dns_cache_hit': self.on_dns_cache_hit,
            'on_dns_cache_miss': self.on_dns_cache_miss,
            'on_task_start': self.on_task_start,
            'on_task_end': self.on_task_end,
            'on_task_error': self.on_task_error,
        }
    
    def add_trace(self, trace_signal: TraceSignal) -> None:
        trace_name = trace_signal.__name__

        if trace_name in self.allowed_traces:
            object.__setattr__(
                self,
                trace_name,
                trace_signal
            )

    async def on_request_start(
        self,
        request: Request,
    ):
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            self.span = None
            return

        http_method = request.method.upper()
        request_span_name = f"HTTP {http_method}"

        request_url = request.url.full

        if callable(self.url_filter):
            request_url = (
                remove_url_credentials(
                    self.url_filter(request.url)
                )
            )
        
        else:
            remove_url_credentials(request.url.full)

        span_attributes = {
            SpanAttributes.HTTP_METHOD: http_method,
            SpanAttributes.HTTP_URL: request_url,
        }

        self.span = self.tracer.start_span(
            request_span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        )

        if callable(self.request_hook):
            self.request_hook(self.span, request)

        self.token = context_api.attach(
            trace.set_span_in_context(self.span)
        )

        self._context_active = True

        inject(request.headers)

    async def on_request_end(
        self,
        response: Response,
    ):
        if self.span is None:
            return

        if callable(self.response_hook):
            self.response_hook(self.span, response)

        if self.span.is_recording() and response.status:
            self.span.set_status(
                Status(
                    http_status_to_status_code(response.status)
                )
            )

            self.span.set_attribute(
                SpanAttributes.HTTP_STATUS_CODE, response.status
            )

        self._end_trace()

    async def on_request_exception(
        self,
        response: Response,
    ) -> None:
        if self.span is None:
            return

        if self.span.is_recording() and response.error:

            if response.status:
                self.span.set_status(
                    Status(StatusCode.ERROR)
                )

            self.span.record_exception(
                str(response.error)
            )

        if callable(self.response_hook):
            self.response_hook(self.span, response)

        self._end_trace()
    
    def _end_trace(self) -> None:
        # context = context_api.get_current()

        if self._context_active:
            context_api.detach(self.token)
            self._context_active = False

            self.span.end()
