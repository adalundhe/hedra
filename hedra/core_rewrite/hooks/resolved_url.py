from ssl import SSLContext
from hedra.core_rewrite.engines.client.client_types.common.url import URL
from pydantic import BaseModel
from typing import Optional


class ResolvedURL(BaseModel):
    ssl_context: Optional[SSLContext]=None
    url: URL

    class Config:
        arbitrary_types_allowed=True