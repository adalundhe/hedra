from typing import Optional
from urllib.parse import urlparse

from hedra.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .url_validator import URLValidator


class URL(OptimizedArg):
    def __init__(self, url: str) -> None:
        super(
            URL,
            self,
        ).__init__()

        URLValidator(value=url)
        self.data = url
        self.parsed = urlparse(url)
        self.optimized: Optional[OptimizedArg] = None

    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data
