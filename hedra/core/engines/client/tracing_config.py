from __future__ import annotations
from hedra.core.engines.types.tracing.tracing_types import (
    RequestHook,
    ResponseHook,
    TraceSignal,
    UrlFilter
)
from hedra.core.engines.types.tracing.url_filters import default_params_strip_filter
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import Optional, Dict, Union


OpenTelemetryTracingConfig = Union[
    UrlFilter,
    RequestHook,
    ResponseHook,
    TraceSignal
]


class TracingConfig:

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

        if url_filter is None:
            url_filter = default_params_strip_filter

        self.url_filter = url_filter
        self.request_hook = request_hook
        self.response_hook = response_hook

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

    def copy(self) -> TracingConfig:
        return TracingConfig(
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
