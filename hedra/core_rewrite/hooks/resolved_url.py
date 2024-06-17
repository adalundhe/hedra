from ssl import SSLContext
from typing import Optional

from pydantic import BaseModel

from hedra.core_rewrite.engines.client.shared.models import URL


class ResolvedURL(BaseModel):
    ssl_context: Optional[SSLContext] = None
    url: URL

    class Config:
        arbitrary_types_allowed = True
