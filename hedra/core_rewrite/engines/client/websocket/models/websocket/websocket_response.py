from hedra.core_rewrite.engines.client.http.models.http import HTTPResponse


class WebsocketResponse(HTTPResponse):

    class Config:
        arbitrary_types_allowed=True
            
 