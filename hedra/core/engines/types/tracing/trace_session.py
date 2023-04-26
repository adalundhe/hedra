from hedra.versioning.flags.types.unstable.flag import unstable
from typing import Dict
from .trace import Trace
from .tracing_types import (
    RequestHook,
    ResponseHook,
    TraceSignal,
    UrlFilter
)


__name__ = 'hedra'
__version__ = "0.7.12"


@unstable
class TraceSession:

    __slots__ = (
        'url_filter',
        'request_hook',
        'response_hook',
        'trace_signals'
    )

    def __init__(
        self,
        url_filter: UrlFilter = None,
        request_hook: RequestHook = None,
        response_hook: ResponseHook = None,
        **kwargs: Dict[str, TraceSignal]
    ) -> None:
        self.url_filter: UrlFilter = url_filter
        self.request_hook: RequestHook = request_hook
        self.response_hook: ResponseHook = response_hook
        self.trace_signals: Dict[str, TraceSignal] = kwargs

    def create_trace(self) -> Trace:

        return Trace(
            url_filter=self.url_filter,
            request_hook=self.request_hook,
            response_hook=self.response_hook,
            **self.trace_signals
        )




