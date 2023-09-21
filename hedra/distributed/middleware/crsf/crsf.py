import zstandard
from base64 import (
    b64encode,
    b64decode
)
from http.cookies import (
    BaseCookie,
    SimpleCookie
)
from hedra.distributed.env import Env, load_env
from hedra.distributed.encryption import AESGCMFernet
from hedra.distributed.middleware.base import (
    Middleware
)
from hedra.distributed.models.http import (
    Request,
    Response
)
from secrets import (
    compare_digest,
    token_urlsafe
)
from typing import (
    Tuple, 
    Optional, 
    List, 
    Set, 
    Dict, 
    Literal
)


class CRSF(Middleware):

    def __init__(
        self,
        secret_bytes_size: Optional[int]=16,
        required_paths: Optional[List[str]] = None,
        exempt_paths: Optional[List[str]] = None,
        sensitive_cookies: Optional[Set[str]] = None,
        safe_methods: List[
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
        ] = [
            "GET", 
            "HEAD", 
            "OPTIONS", 
            "TRACE"
        ],
        cookie_name: str = "csrftoken",
        cookie_path: str = "/",
        cookie_domain: Optional[str] = None,
        cookie_secure: bool = False,
        cookie_httponly: bool = False,
        cookie_samesite: str = "lax",
        header_name: str = "x-csrftoken",
    ) -> None:
        
        env = load_env(Env)

        self.encryptor = AESGCMFernet(env)
        self.secret_bytes_size = secret_bytes_size

        self.required_paths = required_paths
        self.exempt_paths = exempt_paths
        self.sensitive_cookies = sensitive_cookies
        self.safe_methods = safe_methods
        self.cookie_name = cookie_name
        self.cookie_path = cookie_path
        self.cookie_domain = cookie_domain
        self.cookie_secure = cookie_secure
        self.cookie_httponly = cookie_httponly
        self.cookie_samesite = cookie_samesite
        self.header_name = header_name

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        super().__init__(
            self.__class__.__name__,
            response_headers={}
        )

    async def __run__(
        self, 
        request: Request,
        response: Response,
        status: int
    ) -> Tuple[
        Tuple[Response, int],
        bool
    ]:

        crsf_cookie = request.cookies.get(self.cookie_name)

        request_path = request.path

        is_unsafe_method = request.method not in self.safe_methods

        path_is_required = False
        if self.required_paths:
            path_is_required = self._path_is_required(request_path)

        path_is_exempt = False
        if self.exempt_paths:
            path_is_exempt = self._path_is_exempt(request_path)
        
        has_sensitive_cookies = False
        if self.sensitive_cookies:
            has_sensitive_cookies = self._has_sensitive_cookies(request.cookies)

        is_sensitive = is_unsafe_method and not path_is_exempt and has_sensitive_cookies

        if path_is_required or is_sensitive:
            submitted_csrf_token = request.headers.get(self.header_name)

            csrf_tokens_match = False

            try:

                decoded_crsf_cookie: str = self.encryptor.decrypt(
                    self._decompressor.decompress(
                        b64decode(crsf_cookie.encode())
                    )
                )
                decoded_crsf_token: str = self.encryptor.decrypt(
                    self._decompressor.decompress(
                        b64decode(submitted_csrf_token.encode())
                    )
                )

                csrf_tokens_match = compare_digest(
                    decoded_crsf_cookie, 
                    decoded_crsf_token
                )

            except Exception:
                csrf_tokens_match = False

            
            crsf_match_failed = crsf_cookie is None or submitted_csrf_token is None or csrf_tokens_match is False

            if crsf_match_failed:
                return (
                    Response(
                        request.path,
                        request.method,
                        data="CSRF token verification failed"
                    ), 
                    403
                ), False

        crsf_cookie = request.cookies.get(self.cookie_name)

        response_headers = {}

        if crsf_cookie is None:

            cookie: BaseCookie = SimpleCookie()
            cookie_name = self.cookie_name

            crsf_token = self.encryptor.encrypt(
                token_urlsafe(
                    nbytes=self.secret_bytes_size
                ).encode()
            )

            cookie[cookie_name] = b64encode(
                self._compressor.compress(
                    crsf_token
                )
            ).decode()

            cookie[cookie_name]["path"] = self.cookie_path
            cookie[cookie_name]["secure"] = self.cookie_secure
            cookie[cookie_name]["httponly"] = self.cookie_httponly
            cookie[cookie_name]["samesite"] = self.cookie_samesite

            if self.cookie_domain is not None:
                cookie[cookie_name]["domain"] = self.cookie_domain  # pragma: no cover

            response_headers["set-cookie"] = cookie.output(header="").strip()

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
        

    def _has_sensitive_cookies(self, cookies: Dict[str, str]) -> bool:
        
        for sensitive_cookie in self.sensitive_cookies:
            if cookies.get(sensitive_cookie) is not None:
                return True
            
        return False

    def _path_is_required(self, path: str) -> bool:

        for required_url in self.required_paths:
            if required_url in path:
                return True
            
        return False

    def _path_is_exempt(self, path: str) -> bool:
        
        for exempt_path in self.exempt_paths:
            if exempt_path in path:
                return True
            
        return False