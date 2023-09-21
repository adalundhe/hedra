from hedra.distributed.middleware.base import (
    Middleware
)
from hedra.distributed.models.http import (
    Request,
    Response
)
from typing import Union, Optional, List, Literal, Tuple
from .cors_headers import CorsHeaders


class Cors(Middleware):

    def __init__(
        self,
        access_control_allow_origin: List[str]=None,
        access_control_allow_methods: List[
            Literal[
                "GET",
                "HEAD",
                "OPTIONS",
                "POST",
                "PUT",
                "PATCH",
                "DELETE",
                "TRACE"
            ]
        ]=None,
        access_control_expose_headers: Optional[List[str]]=None,
        access_control_max_age: Optional[Union[int, float]]=None,
        access_control_allow_credentials: Optional[bool]=None,
        access_control_allow_headers: Optional[List[str]]=None
    ) -> None:
         
        self._cors_config = CorsHeaders(
            access_control_allow_origin=access_control_allow_origin,
            access_control_expose_headers=access_control_expose_headers,
            access_control_max_age=access_control_max_age,
            access_control_allow_credentials=access_control_allow_credentials,
            access_control_allow_methods=access_control_allow_methods,
            access_control_allow_headers=access_control_allow_headers,
        )
        
        self.origins = self._cors_config.access_control_allow_origin
        self.cors_methods = self._cors_config.access_control_allow_methods
        self.cors_headers = self._cors_config.access_control_allow_headers
        self.allow_credentials = self._cors_config.access_control_allow_credentials

        self.allow_all_origins = '*' in self._cors_config.access_control_allow_origin

        allowed_headers = self._cors_config.access_control_allow_headers
        self.allow_all_headers = False

        if allowed_headers:
            self.allow_all_headers = '*' in allowed_headers

        self.simple_headers = self._cors_config.to_simple_headers()
        self.preflight_headers = self._cors_config.to_preflight_headers()
        self.preflight_explicit_allow_origin = not self.allow_all_origins or self.allow_credentials
        
        super().__init__(
            self.__class__.__name__,
            methods=['OPTIONS'],
            response_headers=self._cors_config.to_headers()
        )
        
    async def __run__(
        self, 
        request: Request,
        response: Optional[Response],
        status: Optional[int]
    ) -> Tuple[
        Tuple[Response, int],
        bool
    ]:

        headers = request.headers
        method = request.method

        origin = headers.get('origin')
        access_control_request_method = headers.get('access-control-request-method')
        access_control_request_headers = headers.get('access-control-request-headers')
        access_control_request_headers = headers.get("access-control-request-headers")

        if method == "OPTIONS" and access_control_request_method:

            response_headers = dict(self.preflight_headers)

            failures: List[str] = []

            if self.allow_all_origins is False and origin not in self.origins:
                failures.append("origin")

            elif self.preflight_explicit_allow_origin:
                response['Access-Control-Allow-Origin'] = origin

            if access_control_request_method not in self.cors_methods:
                failures.append("method")

            if self.allow_all_headers and access_control_request_headers is not None:
                response_headers["Access-Control-Allow-Headers"] = access_control_request_headers
            
            elif access_control_request_headers:

                for header in access_control_request_headers.split(
                    ','
                ):
                    if header.lower().strip() not in self.cors_headers:
                        failures.append("headers")
                        break
                
            if len(failures) > 0:

                failures_message = ', '.join(failures)

                return (
                    Response(
                        request.path,
                        request.method,
                        headers=response_headers,
                        data=f"Disallowed CORS {failures_message}"
                    ),
                    401
                ), False
            
            if response and status:
                response.headers.update(response_headers)

                return (
                    response, 
                    status
                ), False
            
            return (
                Response(
                    request.path,
                    request.method,
                    headers=response_headers,
                    data=None
                ),
                204
            ), False
        
        response_headers = dict(self.simple_headers)

        has_cookie = headers.get('cookie')
        if self.allow_all_origins and has_cookie:
            response_headers['access-control-allow-origin'] = origin

        elif origin in self.origins:
            response_headers['access-control-allow-origin'] = origin
        
        if response and status:
            response.headers.update(response_headers)

            return (
                response,
                status
            ), True

        return (
            Response(
                request.path,
                request.method,
                headers=response_headers,
                data=None
            ), 
            200
        ), True