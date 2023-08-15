from http.cookies import SimpleCookie
from pydantic import (
    BaseModel
)
from typing import (
    Dict, 
    Union
)


class Response:
    def __init__(
        self,
        path: str,
        method: str,
        headers: Dict[str, str]={},
        data: Union[BaseModel, str, None]=None
    ):
        self.path = path
        self.method = method
        self.headers = headers
        self.data = data
        self._cookies: Union[
            Dict[str, str],
            None
        ] = None

    @property
    def cookies(self):

        if self._cookies is None:

            cookies = self.headers.get('cookie')
            self._cookies = {}

            if cookies:

                parsed_cookies = SimpleCookie()
                parsed_cookies.load(cookies)

                self._cookies =  {
                    name: morsel.value for name, morsel in parsed_cookies.items()
                }

        return self._cookies
