from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.common.types import RequestTypes
from .http import HTTPClient


class HTTP2Client(HTTPClient):
    
    def __init__(self, session: MercuryHTTP2Client) -> None:
        super().__init__(session)
        self.request_type = RequestTypes.HTTP2