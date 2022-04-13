import psutil


class BaseSession:

    def __init__(self, pool_size=None, dns_cache_seconds=None, request_timeout=None) -> None:
        self.connection_pool_size = int((psutil.cpu_count(logical=True) * 10**3)/pool_size)
        self.dns_cache_seconds = dns_cache_seconds
        self.request_timeout = request_timeout
