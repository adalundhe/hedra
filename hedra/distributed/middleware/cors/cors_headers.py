from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    StrictBool,
    conlist
)
from typing import (
    Union, 
    List, 
    Literal,
    Optional,
    Dict
)


class CorsHeaders(BaseModel):
    access_control_allow_origin: conlist(
        StrictStr,
        min_items=1
    )
    access_control_expose_headers: Optional[List[StrictStr]]
    access_control_max_age: Optional[Union[StrictInt, StrictFloat]]
    access_control_allow_credentials: Optional[StrictBool]
    access_control_allow_methods: conlist(
        Literal[
            "GET",
            "HEAD",
            "OPTIONS",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "TRACE"
        ],
        min_items=1
    )
    access_control_allow_headers: Optional[List[StrictStr]]

    def to_headers(self):

        cors_headers: Dict[str, str] = {}

        headers = self.dict(exclude_none=True)

        for key, value in headers.items():
            
            header_key = '-'.join([
                segment.capitalize() for segment in key.split('_')
            ])

            if key == 'access_control_allow_origin':
                header_value = ' | '.join(value)

            elif key == 'access_control_max_age':
                header_value = "true" if value else "false"

            else:
                header_value = ', '.join(value)

            cors_headers[header_key] = header_value

        return cors_headers
    
    def to_simple_headers(self):


        allow_all_origins = False        
        allow_all_origins = "*" in self.access_control_allow_origin
        simple_headers: Dict[str, str] = {}

        if allow_all_origins:
            simple_headers["Access-Control-Allow-Origin"] = "*"

        if self.access_control_allow_credentials:
            simple_headers["Access-Control-Allow-Credentials"] = "true"

        if self.access_control_expose_headers:
            simple_headers["Access-Control-Expose-Headers"] = ", ".join(self.access_control_expose_headers)

        return simple_headers

    def to_preflight_headers(self):

        allow_all_origins = "*" in self.access_control_allow_origin

        access_control_allow_headers = self.access_control_allow_headers or []
        allow_all_headers = "*" in access_control_allow_headers

        safe_headers = {"Accept", "Accept-Language", "Content-Language", "Content-Type"}


        preflight_explicit_allow_origin = not allow_all_origins or self.access_control_allow_credentials


        preflight_headers: Dict[str, str] = {}
        if preflight_explicit_allow_origin:
            # The origin value will be set in preflight_response() if it is allowed.
            preflight_headers["Vary"] = "Origin"

        else:
            preflight_headers["Access-Control-Allow-Origin"] = "*"

        preflight_headers.update(
            {
                "Access-Control-Allow-Methods": ", ".join(self.access_control_allow_methods),
                "Access-Control-Max-Age": str(self.access_control_max_age),
            }
        )

        allow_headers = sorted(safe_headers | set(access_control_allow_headers))
        
        if allow_headers and not allow_all_headers:
            preflight_headers["Access-Control-Allow-Headers"] = ", ".join(allow_headers)

        if self.access_control_allow_credentials:
            preflight_headers["Access-Control-Allow-Credentials"] = "true"

        return preflight_headers